package jobs;

import java.io.FileNotFoundException;
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
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import kmeans.Kmeans;
import kmeans.Kmeans.KCentroidInnitMapper;
import kmeans.Kmeans.KCentroidsInnitReducer;
import kmeans.Kmeans.KNormalizer;
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

public class RegexReconMain extends Configured implements Tool {
	public static int usage() {
		System.err.println("Usage: RegexRecon <options> input\n" + "\t[-b <bin_size>]\n" + "\t[-w <wanted_w>]\n"
				+ "\t[-u <unwated_w>]\n" + "\t[-k --cluster cluster-size]\n" + "\t<input(s)> Input files\n"
				+ "Optional Arguments:\n"
				+ "[-b] The value of the number of the bins that divides de data in the histogram.\n"
				+ "[-w] Wanted words: Seek URL whose addresses contains desired words, separeted by comma, no space.\n"
				+ "[-u] Unwanted words: Words that are banned from processing, even if they are together with wanted words, separeted by comma, no space.\n"
				+ "--cluster: Integer value. Set the quantity of clusters, or centroids, to be calculated by the k-means. Default value: 5.\n"
				+ "\n");
		return 2;
	}

	public static String[] setConfArgs(Configuration conf, String[] args, boolean bin_b, boolean klust_b,
			boolean word_b, boolean na_b) throws IllegalArgumentException, IOException {
		conf.set(Setup.WORD_WANTED, "");
		conf.set(Setup.WORD_UNWANTED, "");
		List<String> otherArgs = new ArrayList<>();
		int flag_b = 0, flag_w = 0, flag_u = 0, flag_k = 0, flag_d = 0, k = 0;
		while (k < args.length) {
			if (args[k].equals("-b")) {
				if (flag_b == 1)
					System.exit(usage());
				else
					flag_b++;
				if (bin_b) {
					String bin = args[++k];
					conf.setInt(Setup.HIST_BIN_DIV, Integer.parseInt(bin));
					Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "hist_bin", bin);
				} else
					k++;
			} else if (args[k].equals("-w")) {
				if (flag_w == 1)
					System.exit(usage());
				else
					flag_w++;
				if (word_b)
					conf.set(Setup.WORD_WANTED, args[++k]);
				else
					k++;
			} else if (args[k].equals("-u")) {
				if (flag_u == 1)
					System.exit(usage());
				else
					flag_u++;
				if (word_b)
					conf.set(Setup.WORD_UNWANTED, args[++k]);
				else
					k++;
			} else if (args[k].equals("-k") || args[k].equals("--cluster")) {
				if (flag_k == 1)
					System.exit(usage());
				else
					flag_k++;
				if (klust_b) {
					String kluster = args[++k];
					conf.setInt(Setup.K_CLUSTER_SIZE, Integer.parseInt(kluster));
					Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), Setup.K_CLUSTER_SIZE, kluster);
				} else
					k++;
			} else if (args[k].equals("-d")) {
				if (flag_d == 1)
					System.exit(usage());
				else
					flag_d++;
				if (na_b) {
					k++;
					if(args[k].matches("gau|gauss|gaussian|1"))
						conf.set(Setup.DIST_METHOD, "1");
					else if(args[k].matches("norm|normal|2"))
						conf.set(Setup.DIST_METHOD, "2");
				} else
					k++;
			} else {
				if (args.length - k < 1) {
					System.exit(usage());
				}
				otherArgs.add(args[k++]);
			}
			k++;
		}
		return otherArgs.toArray(new String[otherArgs.size()]);
	}

	public static void setFilePath(Configuration conf, FileSystem fs) throws IOException, FileNotFoundException {
		Path jobs_path = new Path("jobs/" + FilenameGen.dateGen(true, false));
		if (!fs.exists(jobs_path))
			fs.mkdirs(jobs_path);
		String numJobsFiles = String.valueOf(fs.listStatus(jobs_path).length + 1);
		conf.set(Setup.JOB_PATH, jobs_path.toString() + "/job_" + "0".repeat(4 - numJobsFiles.length()) + numJobsFiles);
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		conf.set(Setup.WORD_WANTED, Resources.createFilter(conf.get(Setup.WORD_WANTED)));
		conf.set(Setup.WORD_UNWANTED, Resources.createFilter(conf.get(Setup.WORD_UNWANTED)));
		Long timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		Job job = Job.getInstance(conf, "RegexRecon - File parse");
		MultithreadedMapper.setMapperClass(job, RegexMapper.class);
		MultithreadedMapper.setNumberOfThreads(job, 8);
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
		Path out_url = new Path(conf.get(Setup.JOB_PATH) + "/data.url");
		FileOutputFormat.setOutputPath(job, out_url); // Arquivo de saida
		// Arquivos extra de saída
		MultipleOutputs.addNamedOutput(job, "URL", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "KnownURL", TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "Meta", TextOutputFormat.class, Text.class, Text.class);
		int res = job.waitForCompletion(true) ? 0 : 1;
		if (res == 1)
			return res;
		Resources.recordTime(conf, timenow, "File parse: ");
//		fs.setStoragePolicy(out_url, "LAZY_PERSIST");

		return regexReconSecondPart(conf, fs);
	}

	public static Integer regexReconSecondPart(Configuration conf, FileSystem fs)
			throws IOException, FileNotFoundException, InterruptedException, ClassNotFoundException {
		Long timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		Job job_url = Job.getInstance(conf, "RegexRecon - URL parse");
		job_url.setJarByClass(RegexRecon.class);
		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(new Path(conf.get(Setup.JOB_PATH) + "/data.url"));
		while (lfs.hasNext()) {
			Path path = lfs.next().getPath();
			if (path.getName().matches("(URL-r-)(\\d+)")) {
//				fs.setStoragePolicy(path, "LAZY_PERSIST");
				MultipleInputs.addInputPath(job_url, path, TextInputFormat.class, UrlMapperParse.class);
			} else if (path.getName().matches("(part-r-)(\\d+)")) {
				MultipleInputs.addInputPath(job_url, path, TextInputFormat.class, UrlMapperParam.class);
			}
		}
//		MultipleInputs.addInputPath(job_url, new Path(conf.get(Setup.JOB_PATH)+"/data/URL*"), TextInputFormat.class, UrlMapperParse.class);
//		MultipleInputs.addInputPath(job_url, new Path(conf.get(Setup.JOB_PATH)+"/data/part*"), TextInputFormat.class, UrlMapperIp.class);
		job_url.setCombinerClass(Reducer.class);
		job_url.setReducerClass(UrlReducer.class);
		job_url.setOutputKeyClass(Text.class);
		job_url.setOutputValueClass(Text.class);
		Path url_path = new Path(conf.get(Setup.JOB_PATH) + "/data.url");
		RemoteIterator<FileStatus> lfs_knownurl = fs.listStatusIterator(url_path);
		while (lfs_knownurl.hasNext()) {
			Path path = lfs_knownurl.next().getPath();
			if (path.getName().matches("(KnownURL-r-)(\\d+)")) {
				job_url.addCacheFile(path.toUri());
			}
		}
		FileInputFormat.addInputPath(job_url, new Path(conf.get(Setup.JOB_PATH) + "/data.url"));
		FileOutputFormat.setOutputPath(job_url, new Path(conf.get(Setup.JOB_PATH) + "/data.vector"));
		int res = job_url.waitForCompletion(true) ? 0 : 1;
		if (res == 1)
			return res;
		Resources.recordTime(conf, timenow, "URL parse: ");
		timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		res = histoParse(conf);
		if (res == 1)
			return res;
		Resources.recordTime(conf, timenow, "Histogram: ");
		conf.setInt(Setup.D_PARAM_SIZE, Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), "param"));

		if (!fs.exists(new Path(conf.get(Setup.JOB_PATH) + "/data.norm"))) {
			timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
			res = normParse(conf);
			if (res == 1)
				return res;
			Resources.recordTime(conf, timenow, "Normalization: ");
		}
		Integer kluster = conf.getInt(Setup.K_CLUSTER_SIZE, 5);
		if (!fs.exists(new Path(conf.get(Setup.JOB_PATH) + "/data.centroids" + kluster))) {
			timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
			res = innitCentroids(conf, fs, kluster);
			if (res == 1)
				return res;
			Resources.recordTime(conf, timenow, "Centroid-" + kluster + " Innit: ");
		}
		return res;

	}

	public static Integer histoParse(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job_histo = Job.getInstance(conf, "RegexRecon - Histogram");
		job_histo.setJarByClass(RegexRecon.class);
		job_histo.setMapperClass(NormMapper.class);
		job_histo.setCombinerClass(Reducer.class);
		job_histo.setReducerClass(NormReducer.class);
		job_histo.setOutputKeyClass(Text.class);
		job_histo.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job_histo, new Path(conf.get(Setup.JOB_PATH) + "/data.vector"));
		FileOutputFormat.setOutputPath(job_histo, new Path(conf.get(Setup.JOB_PATH) + "/data.meta"));
		MultipleOutputs.addNamedOutput(job_histo, "Histogram", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job_histo, "VarianceMeta", TextOutputFormat.class, Text.class, Text.class);
		int res = job_histo.waitForCompletion(true) ? 0 : 1;
		return res;
	}

	public static Integer normParse(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job_norm = Job.getInstance(conf, "K-means - Normalization");
		job_norm.setJarByClass(Kmeans.class);
		job_norm.setMapperClass(KNormalizer.class);
		job_norm.setCombinerClass(Reducer.class);
		job_norm.setReducerClass(Reducer.class);
		job_norm.setOutputKeyClass(Text.class);
		job_norm.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job_norm, new Path(conf.get(Setup.JOB_PATH) + "/data.vector"));
		FileOutputFormat.setOutputPath(job_norm, new Path(conf.get(Setup.JOB_PATH) + "/data.norm"));
		LazyOutputFormat.setOutputFormatClass(job_norm, TextOutputFormat.class);
		int res = job_norm.waitForCompletion(true) ? 0 : 1;
		return res;
	}

	public static Integer innitCentroids(Configuration conf, FileSystem fs, Integer kluster)
			throws IOException, FileNotFoundException, InterruptedException, ClassNotFoundException {
		Job job_cinnit = Job.getInstance(conf, "K-means - Centroids Innitializer");
		job_cinnit.setJarByClass(Kmeans.class);
		job_cinnit.setMapperClass(KCentroidInnitMapper.class);
		job_cinnit.setCombinerClass(Reducer.class);
		job_cinnit.setReducerClass(KCentroidsInnitReducer.class);
		job_cinnit.setOutputKeyClass(Text.class);
		job_cinnit.setOutputValueClass(Text.class);
		Path in = new Path(conf.get(Setup.JOB_PATH) + "/data.meta");
		RemoteIterator<FileStatus> lfs1 = FileSystem.get(conf).listStatusIterator(in);
		while (lfs1.hasNext()) {
			Path p_in = lfs1.next().getPath();
			if (p_in.getName().matches("(VarianceMeta-r-)(\\d+)")) {
				FileInputFormat.addInputPath(job_cinnit, p_in);
			}
		}
		Path out = new Path(conf.get(Setup.JOB_PATH) + "/data.centroids" + kluster);
		FileOutputFormat.setOutputPath(job_cinnit, out);
		MultipleOutputs.addNamedOutput(job_cinnit, "Centroids", TextOutputFormat.class, Text.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job_cinnit, TextOutputFormat.class);
		int res = job_cinnit.waitForCompletion(true) ? 0 : 1;
		return res;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1)
			System.exit(usage());
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		setFilePath(conf, fs);
		int res = ToolRunner.run(conf, new RegexReconMain(), setConfArgs(conf, args, true, true, true, true));
		System.exit(res);
	}
}
