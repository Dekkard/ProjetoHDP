package jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import resources.Setup;

public class Recommender extends Configured implements Tool{
	public static class RecMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("	");
			String urls = "";
			if(v.length>1) {
				urls += v[1];
			}
			context.write(new Text(v[0]), new Text(urls));
		}
	}
	public static class RecReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		protected void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			URI[] uris = context.getCacheFiles();
			for(URI uri : uris) {
				Path path = new Path(uri.getPath());
				if(path.getName().matches("(GroupList(\\d+))-m-(\\d+)")) {
					String group_out = "";
		        	Matcher mat = Pattern.compile("(GroupList(\\d+))-m-(\\d+)").matcher(path.getName());
					if(mat.find()) {
						group_out = mat.group(1);
					}
					FSDataInputStream in = fs.open(path);
					BufferedReader br = new BufferedReader(new InputStreamReader(in));
					String known_ip;
					boolean found = false;
			        while ((known_ip = br.readLine()) != null){
			        	if(known_ip==key.toString()) {
			        		for(Text t : lines) {
			    				String[] val = t.toString().split(";NEXT:");
			    				for(String st : val) {
			    					String[] url_list = st.split(":QTD>");
			    					mos.write(group_out, key, new Text(url_list[0]));
			    				}	
			    			}
			        		found=true;
			        		break;
			        	}
			        }
			        if(found) break;
				}
			}
		}
	}
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "Recommendation");
	    job.setJarByClass(Recommender.class);
	    job.setMapperClass(RecMapper.class);
	    job.setCombinerClass(Reducer.class);
	    job.setReducerClass(RecReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    String path_kmeans_job = args[0]+"/data.kmeans";
		RemoteIterator<FileStatus> lfs_kmeans = fs.listStatusIterator(new Path(path_kmeans_job));
	    String path_name = "";	
		Integer k_suf = 0;
    	while(lfs_kmeans.hasNext()) {
			Path p = lfs_kmeans.next().getPath();
			Matcher mat = Pattern.compile("kmeans_(\\d+)").matcher(p.getName());
			if(mat.find()) {
				int suf = Integer.parseInt(mat.group(1));
				if(k_suf<suf) {
					k_suf = suf;
					path_name = p.getName();
				}
			}
		}
	    path_kmeans_job += "/"+path_name;
	    Integer r_suf = 0;
		RemoteIterator<FileStatus> lfs_kmeans_round = fs.listStatusIterator(new Path(path_kmeans_job));
		while(lfs_kmeans_round.hasNext()) {
			Path p = lfs_kmeans_round.next().getPath();
			Matcher mat = Pattern.compile("round_(\\d+)").matcher(p.getName());
			if(mat.find()) {
				int suf = Integer.parseInt(mat.group(1));
				if(r_suf<suf) {
					r_suf = suf;
					path_name = p.getName();
				}
			}
		}
		path_kmeans_job += "/"+path_name;
		RemoteIterator<FileStatus> lfs_groups = fs.listStatusIterator(new Path(path_kmeans_job));
		while(lfs_groups.hasNext()) {
			Path p = lfs_groups.next().getPath();
			if(p.getName().matches("(GroupList(\\d+))-m-(\\d+)")) {
				job.addCacheFile(p.toUri());
				Matcher mat = Pattern.compile("(GroupList(\\d+))-m-(\\d+)").matcher(p.getName());
				if(mat.find()) {
					try {
						MultipleOutputs.addNamedOutput(job, mat.group(1), TextOutputFormat.class, Text.class, Text.class);
					} catch(IllegalArgumentException e) {}
				}
			}
		}	    
    	RemoteIterator<FileStatus> lfs_dataurl = fs.listStatusIterator(new Path(args[0]+"/data.url"));
		while(lfs_dataurl.hasNext()) {
			Path p = lfs_dataurl.next().getPath();
			if(p.getName().matches("URL-r-(\\d+)")) {
				FileInputFormat.addInputPath(job, p);
			}
		}
	    
	    Path path = new Path(conf.get(Setup.JOB_PATH)+"/data.recommended");
	    if(fs.exists(path)) fs.delete(path, true);
		FileOutputFormat.setOutputPath(job, path);
	    return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		if(args.length < 1) System.exit(2);
	    Configuration conf = new Configuration();
	    conf.set(Setup.JOB_PATH, args[args.length-1] );
		int res = ToolRunner.run(conf, new Recommender(), args);
	    System.exit(res);
	}
}
