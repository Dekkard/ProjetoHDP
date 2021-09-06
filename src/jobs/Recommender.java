package jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import resources.Resources;
import resources.Setup;

public class Recommender extends Configured implements Tool {
	public static class RecMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("	");
			String urls = "";
			if (v.length > 1) {
				urls += v[1];
				context.write(new Text(v[0]), new Text("url>" + urls));
			}
		}
	}

	public static class GroupsMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("	");
			context.write(new Text(v[0]), new Text("gl>" + v[1]));
		}
	}

	public static class RecReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;

		protected void setup(Context context) throws IOException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			String groupname = "";
			List<String> url_list = new ArrayList<>();
			for (Text t : lines) {
				Matcher mat = Pattern.compile("(gl>)(.*)|(url>)(.*)").matcher(t.toString());
				if (mat.find()) {
					if (mat.group(1) != null) {
						groupname = mat.group(2);
					} else if (mat.group(3) != null) {
						String[] val = mat.group(4).split(";NEXT:");
						for (String st : val) {
							String[] url = st.split(":QTD>");
							url_list.add(url[0]);
						}
					}
				}
			}
			for (String url : url_list) {
				mos.write("URLList" + groupname, new Text(url), null);
			}
			mos.write("IPList" + groupname, key, null);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "Recommendation");
		job.setJarByClass(Recommender.class);
//		job.setInputFormatClass(MultiInputFormat.class);
//		job.setMapperClass(RecMapper.class);
		job.setCombinerClass(Reducer.class);
		job.setReducerClass(RecReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		String path_kmeans_job = args[0] + "/data.kmeans";
		Comparator<FileStatus> comp = new Comparator<FileStatus>() {
			@Override
			public int compare(FileStatus o1, FileStatus o2) {
				if (o1.getModificationTime() >= o2.getModificationTime())
					return -1;
				else
					return 1;
			}
		};
		FileStatus[] fsts = fs.listStatus(new Path(path_kmeans_job));
		Arrays.sort(fsts, comp);
		FileStatus[] fsts1 = fs.listStatus(fsts[0].getPath());
		Arrays.sort(fsts1, comp);
		Path kpath = new Path(path_kmeans_job + "/" + fsts[0].getPath().getName() + "/" + fsts1[0].getPath().getName());
		Resources.iterateFiles(fs, kpath, "GroupList-r-(\\d+)", (p) -> {
			MultipleInputs.addInputPath(job, p, TextInputFormat.class, GroupsMapper.class);
		});
		Resources.iterateFiles(fs, new Path(args[0] + "/data.url"), "URL-r-(\\d+)", (p) -> {
			MultipleInputs.addInputPath(job, p, TextInputFormat.class, RecMapper.class);
		});
		Integer kluster = Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), Setup.K_CLUSTER_SIZE);
		for (int k = 1; k <= kluster; k++) {
			String k_index = String.valueOf(k);
			k_index = "0".repeat(kluster.toString().length() - k_index.length()) + k_index;
			MultipleOutputs.addNamedOutput(job, "URLList" + k_index, TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "IPList" + k_index, TextOutputFormat.class, Text.class, Text.class);
		}
		Path path = new Path(conf.get(Setup.JOB_PATH) + "/data.recommended");
		if (fs.exists(path))
			fs.delete(path, true);
		FileOutputFormat.setOutputPath(job, path);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1)
			System.exit(2);
		Configuration conf = new Configuration();
		conf.set(Setup.JOB_PATH, args[args.length - 1]);
		int res = ToolRunner.run(conf, new Recommender(), args);
		System.exit(res);
	}
}
