package jobs;

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

public class HistogramMain extends Configured implements Tool {
	public static int usage() {
		System.err.println("Usage: HistogramMain -b <bin_size> -n <normalizer> input\n"
				+ "[-b] The value of the number of the bins that divides de data in the histogram.\n"
				+ "input: the folder where the pre-processed data resides");
		return 2;
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Path p_hist = new Path(conf.get(Setup.JOB_PATH) + "/data.meta");
		if (fs.exists(p_hist))
			fs.delete(p_hist, true);
		Long timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		int res = RegexReconMain.histoParse(conf);
		if (res == 1)
			return res;
		Resources.recordTime(conf, timenow, "Histogram: ");
		return res;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1)
			System.exit(usage());
		Configuration conf = new Configuration();
		conf.set(Setup.JOB_PATH, args[args.length - 1]);
		int res = ToolRunner.run(conf, new HistogramMain(), RegexReconMain.setConfArgs(conf, args, true, false, false, true));
		System.exit(res);
	}
}
