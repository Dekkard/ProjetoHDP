package jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

import resources.Resources;
import resources.Setup;

public class ProjetoHDPMain {
	public static int usage() {
		System.err.println("Usage: ProjetoHDP <options> <in>... <in>\n" + "\t[-b <bin_size>]\n" + "\t[-w <wanted_w>]\n"
				+ "\t[-u <unwated_w>]\n" + "\t[-k,--cluster <cluster-size>]\n" + "\t[-p,--param <num-param>]\n"
				+ "\t[-e,--error <error-threshold>]\n" + "\t[-s,--scale <decimal-unumver>]\n"
				+ "\t[-r,--rounds <num-max-rounds>]\n" + "\t<input(s)> Input files\n" + "Optional Arguments:\n"
				+ "[-b] The value of the number of the bins that divides de data in the histogram.\n"
				+ "[-w] Wanted words: Seek URL whose addresses contains desired words, separeted by comma, no space.\n"
				+ "[-u] Unwanted words: Words that are banned from processing, even if they are together with wanted words, separeted by comma, no space.\n"
				+ "--cluster: Integer value. Set the quantity of clusters, or centroids, to be calculated by the k-means. Default value: 5.\n"
				+ "--param: Integer value. Specifies the number of parameters.\n"
				+ "--error: Double value. Set the comparation threshold between the centroids during execution of K-means. Default threshold value: 5E-15.\n"
				+ "--scale: Integer value. Simplify the Double values to the decimal number passed by this option. Default value: 15th decimal number\n"
				+ "--rounds: Integer value. Determinates the maximum rounds of k-means iteractions tries, alas, the algorithm may end earlier. Default value: 15 max rounds.\n"
				+ "\n");
		return 2;
	}

	public static String[] setConfArgs(Configuration conf, String[] args) throws IllegalArgumentException, IOException {
		conf.set(Setup.WORD_WANTED, "");
		conf.set(Setup.WORD_UNWANTED, "");
//		conf.set(Setup.K_CLUSTER_SIZE, "5");
//	    conf.setInt(Setup.D_PARAM_SIZE, 15);
		conf.set(Setup.ERROR_MARGIN, "5E-15");
		conf.set(Setup.OUTPUT_NAME, "job");
		conf.set(Setup.USCALE, "15");
		List<String> otherArgs = new ArrayList<>();
		int k = 0;
		int flag_b = 0, flag_w = 0, flag_u = 0;
		int flag_k = 0, flag_p = 0, flag_e = 0, flag_s = 0, flag_r = 0;
		while (k < args.length) {
			if (args[k].equals("-b")) {
				if (flag_b == 1)
					System.exit(usage());
				else
					flag_b++;
				String bin = args[++k];
				conf.setInt(Setup.HIST_BIN_DIV, Integer.parseInt(bin));
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "hist_bin", bin);
			} else if (args[k].equals("-w")) {
				if (flag_w == 1)
					System.exit(usage());
				else
					flag_w++;
				conf.set(Setup.WORD_WANTED, args[++k]);
			} else if (args[k].equals("-u")) {
				if (flag_u == 1)
					System.exit(usage());
				else
					flag_u++;
				conf.set(Setup.WORD_UNWANTED, args[++k]);
			} else if (args[k].equals("-k") || args[k].equals("--cluster")) {
				if (flag_k == 1)
					System.exit(usage());
				else
					flag_k++;
				String kluster = args[++k];
				conf.setInt(Setup.K_CLUSTER_SIZE, Integer.parseInt(kluster));
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), Setup.K_CLUSTER_SIZE, kluster);
			} else if (args[k].equals("-p") || args[k].equals("--param")) {
				if (flag_p == 1)
					System.exit(usage());
				else
					flag_p++;
				conf.setInt(Setup.D_PARAM_SIZE, Integer.parseInt(args[++k]));
			} else if (args[k].equals("-e") || args[k].equals("--error")) {
				if (flag_e == 1)
					System.exit(usage());
				else
					flag_e++;
				conf.set(Setup.ERROR_MARGIN, args[++k]);
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

	public static void main(String[] args) throws Exception {
		if (args.length < 1)
			System.exit(usage());
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		RegexReconMain.setFilePath(conf, fs);
		args = setConfArgs(conf, args);
		int res = ToolRunner.run(conf, new RegexReconMain(), args);
		if (res == 1)
			System.exit(res);
		res = ToolRunner.run(conf, new KmeansMain(), args);
		if (res == 1)
			System.exit(res);
		System.exit(res);
	}
}
