package jobs;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import preproc.RegexRecon;
import preproc.RegexRecon.NormMapper;
import preproc.RegexRecon.NormReducer;
import preproc.RegexRecon.RegexMapper;
import preproc.RegexRecon.RegexReducer;
import preproc.RegexRecon.UrlMapperParam;
import preproc.RegexRecon.UrlMapperParse;
import preproc.RegexRecon.UrlReducer;
import resources.FilenameGen;
import resources.Resources;
import resources.Setup;

@SuppressWarnings("all")
public class RegexReconMain extends Configured implements Tool{
	public static int usage() {
		System.err.println("Usage: RegexRecon <options> input\n"
				+ "\t[-b <bin_size>]\n"
				+ "\t[-w <wanted_w>]\n"
				+ "\t[-u <unwated_w>]\n"
				+ "\t<input(s)> Input files\n"
				+ "Optional Arguments:\n"
				+ "[-b] The value of the number of the bins that divides de data in the histogram.\n"
				+ "[-w] Wanted words: Seek URL whose addresses contains desired words, separeted by comma, no space.\n"
				+ "[-u] Unwanted words: Words that are banned from processing, even if they are together with wanted words, separeted by comma, no space.\n\n");
		return 2;
	}
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		conf.set(Setup.WORD_WANTED,Resources.createFilter(conf.get(Setup.WORD_WANTED)));
		conf.set(Setup.WORD_UNWANTED,Resources.createFilter(conf.get(Setup.WORD_UNWANTED)));
		Long timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		Job job = Job.getInstance(conf, "RegexRecon - File parse");
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
		Path out_url = new Path(conf.get(Setup.JOB_PATH)+"/data.url");
		FileOutputFormat.setOutputPath(job, out_url); // Arquivo de saida
		MultipleOutputs.addNamedOutput(job, "URL", TextOutputFormat.class, Text.class, Text.class); // Arquivos extra de saída
		MultipleOutputs.addNamedOutput(job, "KnownURL", TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "Meta", TextOutputFormat.class, Text.class, Text.class);
		int res = job.waitForCompletion(true)?0:1;
		if(res == 1) return res;
		Resources.appendFile(conf, "File parse: "+Resources.timeParse(LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3))-timenow), "Time.meta", conf.get(Setup.JOB_PATH));
//		fs.setStoragePolicy(out_url, "LAZY_PERSIST");
		
		timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		Job job_url = Job.getInstance(conf, "RegexRecon - URL parse");
		job_url.setJarByClass(RegexRecon.class);
		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(new Path(conf.get(Setup.JOB_PATH)+"/data.url"));
		while(lfs.hasNext()) {
			Path path = lfs.next().getPath();
			if(path.getName().matches("(URL-r-)(\\d+)")) {
//				fs.setStoragePolicy(path, "LAZY_PERSIST");
				MultipleInputs.addInputPath(job_url, path , TextInputFormat.class, UrlMapperParse.class);
			}
			else if(path.getName().matches("(part-r-)(\\d+)"))	{
				MultipleInputs.addInputPath(job_url, path , TextInputFormat.class, UrlMapperParam.class);
			}
		}
//		MultipleInputs.addInputPath(job_url, new Path(conf.get(Setup.JOB_PATH)+"/data/URL*"), TextInputFormat.class, UrlMapperParse.class);
//		MultipleInputs.addInputPath(job_url, new Path(conf.get(Setup.JOB_PATH)+"/data/part*"), TextInputFormat.class, UrlMapperIp.class);
		job_url.setCombinerClass(Reducer.class);
		job_url.setReducerClass(UrlReducer.class);
		job_url.setOutputKeyClass(Text.class);
		job_url.setOutputValueClass(Text.class);
		Path url_path = new Path(conf.get(Setup.JOB_PATH)+"/data.url");
		RemoteIterator<FileStatus> lfs_knownurl = fs.listStatusIterator(url_path);
		while(lfs_knownurl.hasNext()) {
			Path path = lfs_knownurl.next().getPath();
			if(path.getName().matches("(KnownURL-r-)(\\d+)")) {
				job_url.addCacheFile(path.toUri());
			}
		}
		FileInputFormat.addInputPath(job_url, new Path(conf.get(Setup.JOB_PATH)+"/data.url"));
		FileOutputFormat.setOutputPath(job_url, new Path(conf.get(Setup.JOB_PATH)+"/data.vector"));
		res = job_url.waitForCompletion(true)?0:1;
		if(res == 1) return res;
		Resources.appendFile(conf,  "URL parse: "+Resources.timeParse(LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3))-timenow), "Time.meta", conf.get(Setup.JOB_PATH));

		timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		Job job_ttl = Job.getInstance(conf, "RegexRecon - Normal parse");
		job_ttl.setJarByClass(RegexRecon.class);
		job_ttl.setMapperClass(NormMapper.class);
		job_ttl.setCombinerClass(Reducer.class);
		job_ttl.setReducerClass(NormReducer.class);
		job_ttl.setOutputKeyClass(Text.class);
		job_ttl.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job_ttl, new Path(conf.get(Setup.JOB_PATH)+"/data.vector"));
		FileOutputFormat.setOutputPath(job_ttl, new Path(conf.get(Setup.JOB_PATH)+"/data.meta"));
		MultipleOutputs.addNamedOutput(job_ttl, "Histogram", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job_ttl, "VarianceMeta", TextOutputFormat.class, Text.class, Text.class);
		res = job_ttl.waitForCompletion(true)?0:1;
		if(res == 1) return res;
		Resources.appendFile(conf,  "Normal parse: "+Resources.timeParse(LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3))-timenow), "Time.meta", conf.get(Setup.JOB_PATH));
		conf.setInt(Setup.D_PARAM_SIZE, Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), "param"));
		return res;
	}
	public static String[] setConfArgs(Configuration conf, String[] args) throws IllegalArgumentException, IOException {
		conf.set(Setup.WORD_WANTED, "");
		conf.set(Setup.WORD_UNWANTED, "");
	    List<String> otherArgs = new ArrayList<>();
	    int flag_b = 0, flag_w = 0, flag_u = 0, k = 0;
	    while(k < args.length) {
	    	if(args[k].equals("-b")) {
	    		if(flag_b == 1) System.exit(usage()); else flag_b++;
	    		String bin = args[++k];
				conf.setInt(Setup.HIST_BIN_DIV, Integer.parseInt(bin));
	    		Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "hist_bin", bin);
	    	} else if(args[k].equals("-w")) {
	    		if(flag_w == 1) System.exit(usage()); else flag_w++;
	    		conf.set(Setup.WORD_WANTED, args[++k]);
	    	} else if(args[k].equals("-u")) {
	    		if(flag_u == 1) System.exit(usage()); else flag_u++;
	    		conf.set(Setup.WORD_UNWANTED, args[++k]);
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
	public static void main(String[] args) throws Exception {
		if(args.length < 1) System.exit(usage());
		
	    Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path jobs_path = new Path("jobs/"+FilenameGen.dateGen(true,false));
	    if(!fs.exists(jobs_path)) fs.mkdirs(jobs_path);
	    String numJobsFiles = String.valueOf(fs.listStatus(jobs_path).length+1);
	    conf.set(Setup.JOB_PATH, jobs_path.toString()+"/job_"+"0".repeat(4-numJobsFiles.length())+numJobsFiles);
	    
		int res = ToolRunner.run(conf, new RegexReconMain(), setConfArgs(conf, args));
	    System.exit(res);
	}
}
