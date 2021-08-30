package tester;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import resources.Resources;
import resources.Setup;

public class FileMeta extends Configured implements Tool {
	public static class MetaMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String ip = null;
			Long time = null;
			
			Matcher mat = Pattern.compile(".*(bot).*",Pattern.CASE_INSENSITIVE).matcher(value.toString());
			if(mat.find()) context.write(new Text("Bots"), new Text("1"));
			
			mat = Pattern.compile(Resources.REGEX_IP,Pattern.CASE_INSENSITIVE).matcher(value.toString());
			if(mat.find()) ip = mat.group(1);
			mat = Pattern.compile(Resources.REGEX_DATE+"[:]"+Resources.REGEX_TIME,Pattern.CASE_INSENSITIVE).matcher(value.toString());
			if(mat.find()) {
				try {
					time = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss",Locale.ENGLISH).parse(mat.group(1)+" "+mat.group(3)).getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			context.write(new Text(ip), new Text(time+";"));
		}	
	}
	public static class MetaReducer extends Reducer<Text, Text, Text, Text> {
		List<String> lines_qtd;
		List<String> ips;
		List<Long> seconds;
		protected void setup(Context context) throws IOException {
			lines_qtd = new ArrayList<>();
			ips = new ArrayList<>();
			seconds = new ArrayList<>();
		}
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			if(key.toString().equals("Bots")) {
				context.write(key, new Text(String.valueOf(Iterables.size(lines))));
			} else {
				for(Text l : lines) {
					String[] v = l.toString().split(";");
					String ip = v[0];
					Long time = Long.parseLong(v[1]);
					if(!ips.contains(ip)) {
						ips.add(ip);
					}
					int i=0;
					for(Long s : seconds) {
						if(time<s) {
							seconds.add(i, time);
							break;
						} else if(i==seconds.size()-1) {
							seconds.add(time);
							break;
						}
						i++;
					}
				}
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("IPs"), new Text(String.valueOf(ips.size())));
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();		
		Job job = Job.getInstance(conf, "Meta");
	    job.setJarByClass(FileMeta.class);
	    job.setMapperClass(MetaMapper.class);
	    job.setCombinerClass(Reducer.class);
	    job.setReducerClass(MetaReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    for(int i=1;i<=args.length;i++) {
	    	FileInputFormat.addInputPath(job, new Path(args[i]));
	    }
	    FileOutputFormat.setOutputPath(job, new Path(conf.get(Setup.JOB_PATH)+"meta"));
	    return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws IOException, Exception {
		if(args.length != 1) {
			System.out.println("One file or directory at a time.");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path jobs_path = new Path("meta");
	    if(!fs.exists(jobs_path)) fs.mkdirs(jobs_path);
	    conf.set(Setup.JOB_PATH, jobs_path.toString()+"/"+args[1]+"Meta");
	    
		int res = ToolRunner.run(conf, new FileMeta(), args);
	    System.exit(res);
	}
}
