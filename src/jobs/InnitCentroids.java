package jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import kmeans.Kmeans;
import kmeans.Kmeans.KCentroidInnitMapper;
import kmeans.Kmeans.KCentroidsInnitReducer;
import resources.Setup;

public class InnitCentroids extends Configured implements Tool{
	public static int usage() {
		System.err.println( "Usage: Kmeans <options> <input>\n"
				+ "\t[-k,--cluster <cluster-size>]\n"
				+ "input: the folder where the pre-processed data resides\n"
				+ "Optional Arguments:\n"
				+ "--cluster: Integer value. Set the quantity of clusters, or centroids, to be calculated by the k-means. Default value: 5.\n\n");
		return 2;
	}
	public static String[] setConfArgs(Configuration conf, String[] args) throws IOException {
//		conf.setInt(Setup.K_CLUSTER_SIZE, 5);
//	    conf.setInt(Setup.D_PARAM_SIZE, 15);
	    List<String> otherArgs = new ArrayList<>();
	    int flag_k = 0, k = 0;
	    while(k < args.length) {
	    	if(args[k].equals("-k")||args[k].equals("--cluster")) {
	    		if(flag_k == 1) System.exit(usage()); else flag_k++;
	    		conf.setInt(Setup.K_CLUSTER_SIZE, Integer.parseInt(args[++k]));
	    	} else {
	    		if(args.length - k < 1) {
	    			System.exit(usage());
	    		}
	    		otherArgs.add(args[k++]);
	    	}
	    	k++;
	    }
		return otherArgs.toArray(new String[otherArgs.size()]);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Integer kluster = conf.getInt(Setup.K_CLUSTER_SIZE,5);
//	    if(!fs.exists(new Path(conf.get(Setup.JOB_PATH)+"/data.norm"))) {
	    Job job_centroidinnit = Job.getInstance(conf, "K-means - Centroids Innitializer");
	    job_centroidinnit.setJarByClass(Kmeans.class);
	    job_centroidinnit.setMapperClass(KCentroidInnitMapper.class);
	    job_centroidinnit.setCombinerClass(Reducer.class);
	    job_centroidinnit.setReducerClass(KCentroidsInnitReducer.class);
	    job_centroidinnit.setOutputKeyClass(Text.class);
	    job_centroidinnit.setOutputValueClass(Text.class);
	    Path in = new Path(conf.get(Setup.JOB_PATH)+"/data.meta");
	    RemoteIterator<FileStatus> lfs = FileSystem.get(conf).listStatusIterator(in);
	    while(lfs.hasNext()) {
			Path p_in = lfs.next().getPath();
			if(p_in.getName().matches("(VarianceMeta-r-)(\\d+)")) {
				FileInputFormat.addInputPath(job_centroidinnit, p_in);
			}
		}
	    Path out = new Path(conf.get(Setup.JOB_PATH)+"/data.centroids"+kluster);
	    if(fs.exists(out)) fs.delete(out, true);
		FileOutputFormat.setOutputPath(job_centroidinnit, out);
		MultipleOutputs.addNamedOutput(job_centroidinnit,"Centroids", TextOutputFormat.class, Text.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job_centroidinnit, TextOutputFormat.class);
	    int res = job_centroidinnit.waitForCompletion(true)?0:1;
	    return res;
	}
	public static void main(String[] args) throws Exception {
		if(args.length < 1) System.exit(usage());
		Configuration conf = new Configuration();
		conf.set(Setup.JOB_PATH, args[args.length-1]);
		int res = ToolRunner.run(conf, new InnitCentroids(), setConfArgs(conf,args));
	    System.exit(res);
	}
}
