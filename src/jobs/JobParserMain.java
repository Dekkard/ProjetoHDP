package jobs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import resources.Setup;

public class JobParserMain extends Configured implements Tool {
	public static int usage() {
		System.err.println("Usage: JobParser <options> input\n" + "\t[-b <bin_size>]\n" + "\t<input(s)> Input files\n"
				+ "Optional Arguments:\n"
				+ "[-b] The value of the number of the bins that divides de data in the histogram.\n" + "\n");
		return 2;
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		return RegexReconMain.regexReconSecondPart(conf, fs);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1)
			System.exit(usage());
		Configuration conf = new Configuration();
		conf.set(Setup.JOB_PATH, args[args.length - 1]);
		int res = ToolRunner.run(conf, new JobParserMain(), RegexReconMain.setConfArgs(conf, args));
		System.exit(res);
	}
}
