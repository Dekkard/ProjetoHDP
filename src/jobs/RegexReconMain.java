package jobs;

import java.io.IOException;

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

import preproc.RegexRecon;
import preproc.RegexRecon.MidMapper;
import preproc.RegexRecon.MidReducer;
import preproc.RegexRecon.RegexMapper;
import preproc.RegexRecon.RegexReducer;
import resources.FilenameGen;
import resources.Setup;

public class RegexReconMain extends Configured implements Tool{
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
	    if ( args.length < 1) {
	      System.err.println("Usage: RegexRecon input(s) directory");
	      return 2;
	    }
		Job job = Job.getInstance(conf, "Regex Recon - Phase One");
		job.setJarByClass(RegexRecon.class); // Classe no qual o Job será baseado
	    job.setMapperClass(RegexMapper.class); // Classe mapeadora
	    job.setCombinerClass(Reducer.class); // Classe combinadora
	    job.setReducerClass(RegexReducer.class); // Classe reduzidora
//	    job.setInputFormatClass();
//	    job.setOutputFormatClass();
	    job.setOutputKeyClass(Text.class); // Classes que lidam com os tipos de chave-valor
	    job.setOutputValueClass(Text.class);
		for (int i = 0; i < args.length; ++i) { // Aceita mais de um diretório ou arquivo [?]
			FileInputFormat.addInputPath(job, new Path(args[i])); // Arquivos de entrada
	    }
		FileOutputFormat.setOutputPath(job, new Path(conf.get(Setup.JOB_PATH)+"/data")); // Arquivo de saida
	    
		int res = job.waitForCompletion(true)?0:1;
		if(res == 0) {
			Job job_mid = Job.getInstance(conf, "Regex Recon - Middle Phase");
			job_mid.setJarByClass(RegexRecon.class);
			job_mid.setMapperClass(MidMapper.class);
			job_mid.setCombinerClass(Reducer.class);
			job_mid.setReducerClass(MidReducer.class);
			job_mid.setOutputKeyClass(Text.class);
			job_mid.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job_mid, new Path(conf.get(Setup.JOB_PATH)+"/data"));
			FileOutputFormat.setOutputPath(job_mid, new Path(conf.get(Setup.JOB_PATH)+"/data.var"));
			return job_mid.waitForCompletion(true)?0:1;
		} else {
			return 1;
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path jobs_path = new Path("jobs_"+FilenameGen.dateGen(true,false));
	    if(!fs.exists(jobs_path)) fs.mkdirs(jobs_path);
	    String numJobsFiles = String.valueOf(fs.listStatus(jobs_path).length+1);
	    conf.set(Setup.JOB_PATH, jobs_path.getName()+"/job_"+"0".repeat(4-numJobsFiles.length())+numJobsFiles);
	    
		int res = ToolRunner.run(conf, new RegexReconMain(), args);
	    System.exit(res);
	}
}
