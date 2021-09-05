package jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import kmeans.Kmeans;
import kmeans.Kmeans.KLoadCentroidMapper;
import kmeans.Kmeans.KMapper;
import kmeans.Kmeans.KReducer;
import resources.Resources;
import resources.Setup;

public class KmeansMain extends Configured implements Tool {
	public static int usage() {
		System.err.println("Usage: Kmeans <options> <input>\n" + "\t[-k,--cluster <cluster-size>]\n"
				+ "\t[-p,--param <num-param>]\n" + "\t[-e,--error <error-threshold>]\n"
				+ "\t[-s,--scale <decimal-unumver>]\n" + "\t[-r,--rounds <num-max-rounds>]\n"
				+ "input: the folder where the pre-processed data resides\n" + "Optional Arguments:\n"
				+ "--cluster: Integer value. Set the quantity of clusters, or centroids, to be calculated by the k-means. Default value: 5.\n"
				+ "--param: Integer value. Specifies the number of parameters.\n"
				+ "--error: Double value. Set the comparation threshold between the centroids during execution of K-means. Default threshold value: 5E-15.\n"
				+ "--scale: Integer value. Simplify the Double values to the decimal number passed by this option. Default value: 15th decimal number\n"
				+ "--rounds: Integer value. Determinates the maximum rounds of k-means iteractions tries, alas, the algorithm may end earlier. Default value: 15 max rounds.\n"
				+ "\n");
		return 2;
	}

	public static String[] setConfArgs(Configuration conf, String[] args) throws IOException {
//		conf.setInt(Setup.K_CLUSTER_SIZE, 5);
//	    conf.setInt(Setup.D_PARAM_SIZE, 15);
		conf.setInt(Setup.D_PARAM_SIZE, Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), "param"));
		conf.setInt(Setup.K_CLUSTER_SIZE,
				Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), Setup.K_CLUSTER_SIZE));
		conf.setDouble(Setup.ERROR_MARGIN, 5E-15);
		conf.set(Setup.USCALE, "15");
		List<String> otherArgs = new ArrayList<>();
		int flag_k = 0, flag_p = 0, flag_e = 0, flag_s = 0, flag_r = 0, k = 0;
		while (k < args.length) {
			if (args[k].equals("-k") || args[k].equals("--cluster")) {
				if (flag_k == 1)
					System.exit(usage());
				else
					flag_k++;
				String kluster = args[++k];
				conf.setInt(Setup.K_CLUSTER_SIZE, Integer.parseInt(kluster));
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), Setup.K_CLUSTER_SIZE, kluster);
			} else if (args[k].equals("-e") || args[k].equals("--error")) {
				if (flag_e == 1)
					System.exit(usage());
				else
					flag_e++;
				conf.set(Setup.ERROR_MARGIN, args[++k]);
			} else if (args[k].equals("-p") || args[k].equals("--param")) {
				if (flag_p == 1)
					System.exit(usage());
				else
					flag_p++;
				conf.setInt(Setup.D_PARAM_SIZE, Integer.parseInt(args[++k]));
			} else if (args[k].equals("-s") || args[k].equals("--scale")) {
				if (flag_s == 1)
					System.exit(usage());
				else
					flag_s++;
				conf.set(Setup.USCALE, args[++k]);
			} else if (args[k].equals("-r") || args[k].equals("--rounds")) {
				if (flag_r == 1)
					System.exit(usage());
				else
					flag_r++;
				conf.setInt(Setup.MAX_ROUNDS, Integer.parseInt(args[++k]));
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

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Long timenow;
		int res = 0;
		Integer kluster = 5;
		kluster = conf.getInt(Setup.K_CLUSTER_SIZE, 10);
		int kmeans_index = 1;
		Path kmeans_datapath = new Path(conf.get(Setup.JOB_PATH) + "/data.kmeans");
		if (fs.exists(kmeans_datapath)) {
			RemoteIterator<FileStatus> lfs_k = fs.listStatusIterator(kmeans_datapath);
			while (lfs_k.hasNext()) {
				if (lfs_k.next().getPath().getName().matches("(kmeans_)(\\d+)"))
					kmeans_index++;
			}
		}
		String kmeans_suffix = "0".repeat(2 - String.valueOf(kmeans_index).length())
				.concat(String.valueOf(kmeans_index));
		conf.set(Setup.JOB_PATH_KMEANS, conf.get(Setup.JOB_PATH) + "/data.kmeans/kmeans_" + kmeans_suffix);
		List<Integer> compare_centroids = new ArrayList<>();
		int blk_score_flag = 0;
		int rounds = 1;
		while (rounds <= conf.getInt(Setup.MAX_ROUNDS, 15)) {
			String round_suffix = "0".repeat(2 - String.valueOf(rounds).length()).concat(String.valueOf(rounds));
			conf.set(Setup.JOB_PATH_KMEANS_ROUND, conf.get(Setup.JOB_PATH_KMEANS) + "/round_" + round_suffix);
			/*
			 * conf.set(Setup.CENTROID_CUR_PATH,conf.get(Setup.JOB_PATH)+"/kmeans_"+
			 * kmeans_suffix+"/round_"+rounds); String centroid_path =
			 * conf.get(Setup.CENTROID_CUR_PATH); if(rounds>1) { FileUtil.copy(fs, new
			 * Path(conf.get(Setup.JOB_PATH)+"/kmeans_"+kmeans_suffix+"/round_"+(rounds-1)+
			 * "/"+Setup.NEW_CENTROID), fs, new Path(centroid_path+"/"+Setup.CENTROID),
			 * false, true, conf); FileUtil.copy(fs, new
			 * Path(centroid_path+"/"+Setup.CENTROID), fs, new
			 * Path(centroid_path+"/"+Setup.NEW_CENTROID), false, true, conf); } else {
			 * Resources.initCentroid(conf,Setup.CENTROID);
			 * Resources.initCentroid(conf,Setup.NEW_CENTROID); }
			 */
			String output_name = conf.get(Setup.JOB_PATH_KMEANS_ROUND);
			Path output = new Path(output_name);
			String centroid_path = "";
			if (rounds > 1) {
				int pr[] = { 1 };
				boolean opt[] = { true };
				while (opt[0]) {
					int prev_round = rounds - pr[0];
					if (prev_round > 1) {
						String rs = "0".repeat(2 - String.valueOf(prev_round).length())
								.concat(String.valueOf(prev_round));
						centroid_path = conf.get(Setup.JOB_PATH_KMEANS) + "/round_" + rs/* +"/compare" */;
						Resources.iterateFiles(fs, new Path(centroid_path), "Centroids-r-(\\d+)", (p) -> {
							try {
								if (fs.getFileStatus(p).getLen() == 0)
									pr[0] += 1;
								else
									opt[0] = false;
							} catch (IOException e) {
							}
						});
					} else {
						centroid_path = conf.get(Setup.JOB_PATH) + "/data.centroids" + kluster;
						opt[0] = false;
					}
				}
			} else {
				centroid_path = conf.get(Setup.JOB_PATH) + "/data.centroids" + kluster;
			}
			conf.set(Setup.CENTROID_CUR_PATH, centroid_path);

			timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
			Job job_k = Job.getInstance(conf, "K-means - Round " + rounds);
			job_k.setJarByClass(Kmeans.class);
			RemoteIterator<FileStatus> lst_centroid = fs.listStatusIterator(new Path(centroid_path));
			while (lst_centroid.hasNext()) {
				Path path = lst_centroid.next().getPath();
				if (path.getName().matches("(Centroids-r-)(\\d+)"))
					MultipleInputs.addInputPath(job_k, path, TextInputFormat.class, KLoadCentroidMapper.class);
			}
			RemoteIterator<FileStatus> lst_norm = fs
					.listStatusIterator(new Path(conf.get(Setup.JOB_PATH) + "/data.norm"));
			while (lst_norm.hasNext()) {
				Path path = lst_norm.next().getPath();
				if (path.getName().matches("(part-r-)(\\d+)"))
					MultipleInputs.addInputPath(job_k, path, TextInputFormat.class, KMapper.class);
			}
//			job_k.setMapperClass(KMapper.class);
			job_k.setCombinerClass(Reducer.class);
			job_k.setReducerClass(KReducer.class); // KReducer
			job_k.setOutputKeyClass(Text.class);
			job_k.setOutputValueClass(Text.class);
//		    FileInputFormat.addInputPath(job_k, new Path(conf.get(Setup.JOB_PATH)+"/data.norm"));
			FileOutputFormat.setOutputPath(job_k, output);
			LazyOutputFormat.setOutputFormatClass(job_k, TextOutputFormat.class);
			MultipleOutputs.addNamedOutput(job_k, "Centroids", TextOutputFormat.class, Text.class, Text.class);
			Resources.iterateFiles(fs, new Path(centroid_path), "Centroids-r-(\\d+)", (p) -> {
				job_k.addCacheFile(p.toUri());
			});
			for (int k = 1; k <= kluster; k++) {
				String k_index = String.valueOf(k);
				k_index = "0".repeat(String.valueOf(kluster).length() - k_index.length()) + k_index;
				MultipleOutputs.addNamedOutput(job_k, "GroupList" + k_index, TextOutputFormat.class, Text.class,
						Text.class);
			}
			MultipleOutputs.addNamedOutput(job_k, "Compare", TextOutputFormat.class, Text.class, Text.class);
			res = job_k.waitForCompletion(true) ? 0 : 1;
			if (res == 1)
				return res;
			Resources.appendFile(conf,
					"K-means_" + kmeans_suffix + "-round_" + round_suffix + ": "
							+ Resources.timeParse(LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3)) - timenow),
					"Time.meta", conf.get(Setup.JOB_PATH));
			/*
			 * if(!Resources.compareLists(conf, Setup.CENTROID,Setup.NEW_CENTROID,
			 * centroid_path, centroid_path)) { FileUtil.copy(fs, new
			 * Path(centroid_path+"/"+Setup.NEW_CENTROID), fs, new
			 * Path(conf.get(Setup.JOB_PATH)+"/centroids.final"), false, true, conf); return
			 * 0; }
			 */

			/*
			 * Job job_comp = Job.getInstance(conf, "K-means - Compare");
			 * job_comp.setJarByClass(Kmeans.class); //
			 * job_comp.setMapperClass(KMapper.class); RemoteIterator<FileStatus>
			 * lst_centroid = fs.listStatusIterator(new Path(centroid_path));
			 * while(lst_centroid.hasNext()) { Path path = lst_centroid.next().getPath();
			 * if(path.getName().matches("(Centroids-r-)(\\d+)"))
			 * MultipleInputs.addInputPath(job_comp, path, TextInputFormat.class,
			 * KCentroidComparatorMapper.class); } RemoteIterator<FileStatus>
			 * lst_newcentroid = fs.listStatusIterator(new
			 * Path(conf.get(Setup.JOB_PATH_KMEANS_ROUND)));
			 * while(lst_newcentroid.hasNext()) { Path path =
			 * lst_newcentroid.next().getPath();
			 * if(path.getName().matches("(Centroids-r-)(\\d+)"))
			 * MultipleInputs.addInputPath(job_comp, path, TextInputFormat.class,
			 * KNewCentroidComparatorMapper.class); }
			 * job_comp.setCombinerClass(Reducer.class);
			 * job_comp.setReducerClass(KCentroidComparatorReducer.class);
			 * job_comp.setOutputKeyClass(Text.class);
			 * job_comp.setOutputValueClass(Text.class); Path compare_path = new
			 * Path(conf.get(Setup.JOB_PATH_KMEANS_ROUND)+"/compare");
			 * FileOutputFormat.setOutputPath(job_comp, compare_path);
			 * LazyOutputFormat.setOutputFormatClass(job_comp, TextOutputFormat.class);
			 * MultipleOutputs.addNamedOutput(job_comp, "Centroids", TextOutputFormat.class,
			 * Text.class, Text.class); MultipleOutputs.addNamedOutput(job_comp, "Compare",
			 * TextOutputFormat.class, Text.class, Text.class); res =
			 * job_comp.waitForCompletion(true)?0:1; if(res==1) return res;
			 */

			Path compare_path = new Path(conf.get(Setup.JOB_PATH_KMEANS_ROUND));
			boolean break_kmeans = false;
//			boolean break_file_survey = false;
			int compare_list_index = 0;
//			int compare_complete_flag = 0;
			int white_score = 0;
			int black_score = 0;
			RemoteIterator<FileStatus> compare_files_centroid = fs.listStatusIterator(compare_path);
			while (compare_files_centroid.hasNext()) {
				Path p = compare_files_centroid.next().getPath();
				if (p.getName().matches("(Compare-r-)(\\d+)")) {
					FSDataInputStream in = fs.open(p);
					BufferedReader br = new BufferedReader(new InputStreamReader(in));
					String rl;
					while ((rl = br.readLine()) != null) {
						String[] comp = rl.split("	");
						String centroid_number = comp[0];
						int centroid_compare = Integer.parseInt(comp[1]);
						if (rounds > 1) {
							Integer centroid_compare_old = compare_centroids.get(compare_list_index);
							System.out.print("Replace " + centroid_number + " -> " + centroid_compare + " to "
									+ centroid_compare_old);
//							int dif = compare_centroids.get(i);
//							compare_centroids.set(i,dif-Integer.parseInt(comp[0]));
//							if(centroid_compare == 0) {
							if (compare_centroids.get(compare_list_index) == centroid_compare) {
//									break_file_survey = true;
//									break;
//								compare_complete_flag++;
								if (centroid_compare == 0)
									white_score++;
								else if (centroid_compare == 1)
									black_score++;
							}
							System.out.println(", completion: " + white_score); // compare_complete_flag
							compare_centroids.set(compare_list_index, centroid_compare);
						} else {
							System.out.println("Add " + centroid_number + " -> " + centroid_compare);
							compare_centroids.add(centroid_compare);
						}
						compare_list_index++;
					}
					int compare_score;
					if (black_score > 0)
						blk_score_flag++;
					if (blk_score_flag >= 5)
						compare_score = white_score + black_score;
					else
						compare_score = white_score;
					System.out.println("Score: " + compare_score + " (" + white_score + "/" + black_score + "/"
							+ blk_score_flag + ")" + " - " + kluster); // compare_complete_flag
					if (compare_score == kluster) { // compare_complete_flag
//						break_file_survey = true;
						System.out.println("Stop");
						break_kmeans = true;
						break;
					} else {
						System.out.println("Continue");
						TimeUnit.SECONDS.sleep(1);
					}
//					if(break_file_survey) break;
				}
			}
			if (break_kmeans)
				break;
			rounds++;
		}
		return res;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1)
			System.exit(usage());
		Configuration conf = new Configuration();
		conf.set(Setup.JOB_PATH, args[args.length - 1]);
		int res = ToolRunner.run(conf, new KmeansMain(), setConfArgs(conf, args));
		System.exit(res);
	}
}
