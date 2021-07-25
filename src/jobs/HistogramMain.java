package jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import preproc.Histogram;
import preproc.Histogram.NormGauMapper;
import preproc.Histogram.NormGauReducer;
import preproc.Histogram.NormMaxMapper;
import preproc.Histogram.NormMaxReducer;
import resources.Setup;

public class HistogramMain extends Configured implements Tool{
	public static int usage() {
		System.err.println("Usage: HistogramMain -b <bin_size> -n <normalizer> input\n"
				+ "[-b] The value of the number of the bins that divides de data in the histogram.\n"
				+ "[-n] Specifies the normalization algorithm, currently there is the maximum and gaussian normalization.\n"
				+ "input: the folder where the pre-processed data resides");
		return 2;
	}
	public static String[] setConfArgs(Configuration conf, String[] args) {
	    List<String> otherArgs = new ArrayList<>();
	    int flag_b = 0, flag_n = 0, k = 0;
	    while(k < args.length) {
	    	if(args[k].equals("-b")) {
	    		if(flag_b == 1) System.exit(usage()); else flag_b++;
	    		conf.setInt(Setup.HIST_BIN_DIV, Integer.parseInt(args[++k]));
	    	} else if(args[k].equals("-n")){
	    		if(flag_n == 1) System.exit(usage()); else flag_n++;
	    		k++;
	    		Matcher m1 = Pattern.compile("(max)(.*)",Pattern.CASE_INSENSITIVE).matcher(args[k]);
	    		Matcher m2 = Pattern.compile("(gau)(.*)",Pattern.CASE_INSENSITIVE).matcher(args[k]);
	    		if(m1.find()||args[k].equals("1")) {
	    			conf.setInt(Setup.NORM_ALG, 1);
	    		} else if(m2.find()||args[k].equals("2")) {
	    			conf.setInt(Setup.NORM_ALG, 2);
	    		}
//	    		if(args[k].equals("maximum")||args[k].equals("1")) conf.setInt(Setup.NORM_ALG, 1);
//	    		else if(args[k].equals("gaussian")||args[k].equals("2")) conf.setInt(Setup.NORM_ALG, 2);
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
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
	    if ( args.length < 1) {
    		return usage();
	    }
	    int normType = conf.getInt(Setup.NORM_ALG, 1); 
		Job job = Job.getInstance(conf, "Histogram"+(normType==1?"Maximun":"Gaussian"));
		job.setJarByClass(Histogram.class);
		Path p = null;
		if(normType == 1) {
			job.setMapperClass(NormMaxMapper.class);
			job.setCombinerClass(Reducer.class);
		    job.setReducerClass(NormMaxReducer.class);
		    p = new Path(conf.get(Setup.JOB_PATH)+"/histogramMax");
		} else if(normType == 2) {
			job.setMapperClass(NormGauMapper.class);
			job.setCombinerClass(Reducer.class);
		    job.setReducerClass(NormGauReducer.class);
		    p = new Path(conf.get(Setup.JOB_PATH)+"/histogramGau");
		}
		if(fs.exists(p)) fs.delete(p,true);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(conf.get(Setup.JOB_PATH)+"/data"));
		FileOutputFormat.setOutputPath(job, p);
	    
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if ( args.length < 1) {
			System.exit(usage());
	    }
		conf.set(Setup.JOB_PATH, args[args.length-1]);
		int res = ToolRunner.run(conf, new HistogramMain(), setConfArgs(conf, args));
		System.exit(res);
	}
}
