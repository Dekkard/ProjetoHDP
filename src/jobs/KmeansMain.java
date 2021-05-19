package jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import kmeans.Kmeans;
import kmeans.Kmeans.KMapper;
import kmeans.Kmeans.KReducer;
import resources.Resources;
import resources.Setup;

public class KmeansMain extends Configured implements Tool {
	public static int usage() {
		System.err.println( "Usage: kmeans <options> <in>... <in>\n"
				+ "\t[-k,--cluster <cluster-size>]\n"
				+ "\t[-e,--error <error-threshold>]\n"
				+ "\t[-s,--scale <decimal-unumver>]\n"
				+ "\t<input(s)>\n\n"
				+ "Optional Arguments:\n"
				+ "--cluster: Integer value. Set the quantity of clusters, or centroids, to be calculated by the k-means. Default value: 5.\n"
				+ "--error: Double value. Set the comparation threshold between the centroids during execution of K-means. Default threshold value: 5E-15.\n"
				+ "--scale: Integer value. Simplify the Double values to the decimal number passed by this option. Default value: 15th decimal number");
		return 2;
	}
	public static String[] setConfArgs(Configuration conf, String[] args) {
		conf.set(Setup.K_CLUSTER_SIZE, "5");
	    conf.set(Setup.D_PARAM_SIZE, "6");
	    conf.set(Setup.ERROR_MARGIN, "5E-15");
	    conf.set(Setup.USCALE, "15");
	    List<String> otherArgs = new ArrayList<>();
	    int flag_k = 0, flag_e = 0, flag_s = 0, k = 0;
	    while(k < args.length) {
	    	if(args[k].equals("-k")||args[k].equals("--cluster")) {
	    		if(flag_k == 1) System.exit(usage()); else flag_k++;
	    		conf.set(Setup.K_CLUSTER_SIZE, args[++k]);
	    	} else if(args[k].equals("-e")||args[k].equals("--error")) {
	    		if(flag_e == 1) System.exit(usage()); else flag_e++;
	    		conf.set(Setup.ERROR_MARGIN, args[++k]);
	    	} else if(args[k].equals("-s")||args[k].equals("--scale")) {
	    		if(flag_s == 1) System.exit(usage()); else flag_s++;
	    		conf.set(Setup.ERROR_MARGIN, args[++k]);
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
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
	    if (args.length < 1) {
	    	return usage();
	    }
	    if(!fs.exists(new Path(conf.get(Setup.JOB_PATH)+"/data"))) return usage();
	    int kmeans_index = 1;
		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(new Path(conf.get(Setup.JOB_PATH)));
		while(lfs.hasNext()) {
			if(lfs.next().getPath().getName().matches("(kmeans_)(\\d+)")) kmeans_index++;
		}
	    int rounds = 1;
		while(true) {
			String kmeans_suffix = "0".repeat(2-String.valueOf(kmeans_index).length()).concat(String.valueOf(kmeans_index));
			conf.set(Setup.CENTROID_CUR_PATH,conf.get(Setup.JOB_PATH)+"/kmeans_"+kmeans_suffix+"/round_"+rounds);
			String centroid_path = conf.get(Setup.CENTROID_CUR_PATH);
			if(rounds>1) {
				FileUtil.copy(fs, new Path(conf.get(Setup.JOB_PATH)+"/kmeans_"+kmeans_suffix+"/round_"+(rounds-1)+"/"+Setup.NEW_CENTROID), 
							  fs, new Path(centroid_path+"/"+Setup.CENTROID), false, true, conf);	
				FileUtil.copy(fs, new Path(centroid_path+"/"+Setup.CENTROID), 
						  	  fs, new Path(centroid_path+"/"+Setup.NEW_CENTROID), false, true, conf);
			} else {
				Resources.initCentroid(conf,Setup.CENTROID);
				Resources.initCentroid(conf,Setup.NEW_CENTROID);
			}
			Path output = new Path(conf.get(Setup.JOB_PATH)+"/data_kmeans");
			if(fs.exists(output)) fs.delete(output,true);
			
			Job job = Job.getInstance(conf, "K-means - Phase Two - Round "+(rounds++));
			job.setJarByClass(Kmeans.class);
		    job.setMapperClass(KMapper.class);
		    job.setCombinerClass(Reducer.class);
		    job.setReducerClass(KReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(conf.get(Setup.JOB_PATH)+"/data"));
		    FileOutputFormat.setOutputPath(job, output);
			job.waitForCompletion(true);
			if(!Resources.compareLists(conf, Setup.CENTROID,Setup.NEW_CENTROID, centroid_path, centroid_path)) {
				FileUtil.copy(fs, new Path(centroid_path+"/"+Setup.NEW_CENTROID), 
					  	  	  fs, new Path(conf.get(Setup.JOB_PATH)+"/centroids.final"), false, true, conf);
				return 0;
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(Setup.JOB_PATH, args[args.length-1]);
		int res = ToolRunner.run(conf, new KmeansMain(), setConfArgs(conf,args));
	    System.exit(res);
	}
}
