package jobs;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import resources.Resources;
import resources.Setup;

public class InnitCentroids extends Configured implements Tool {
	public static int usage() {
		System.err.println("Usage: Kmeans <options> <input>\n" + "\t[-k,--cluster <cluster-size>]\n"
				+ "input: the folder where the pre-processed data resides\n" + "Optional Arguments:\n"
				+ "--cluster: Integer value. Set the quantity of clusters, or centroids, to be calculated by the k-means. Default value: 5.\n\n");
		return 2;
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Integer kluster = conf.getInt(Setup.K_CLUSTER_SIZE, 5);
//	    if(!fs.exists(new Path(conf.get(Setup.JOB_PATH)+"/data.norm"))) {
		Path out = new Path(conf.get(Setup.JOB_PATH) + "/data.centroids" + kluster);
		if (fs.exists(out))
			fs.delete(out, true);
		Long timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		int res = RegexReconMain.innitCentroids(conf, fs, kluster);
		if (res == 1)
			return res;
		recordTime(conf, timenow, "Centroid-" + kluster + " Innit: ");
		return res;
	}
	
	private static void recordTime(Configuration conf, Long timenow, String name) throws IOException {
		Resources.appendFile(conf,
				name + Resources.timeParse(LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3)) - timenow),
				"Time.meta", conf.get(Setup.JOB_PATH));
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1)
			System.exit(usage());
		Configuration conf = new Configuration();
		conf.set(Setup.JOB_PATH, args[args.length - 1]);
		int res = ToolRunner.run(conf, new InnitCentroids(), RegexReconMain.setConfArgs(conf, args));
		System.exit(res);
	}
}
