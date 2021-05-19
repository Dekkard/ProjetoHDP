package jobs;

import org.apache.hadoop.util.ProgramDriver;

public class JobsDriver {
	public static void main(String argv[]) {
		int exitCode = -1;
	    ProgramDriver jd = new ProgramDriver();
	    try {
	    	jd.addClass("ProjetoHDP", ProjetoHDPMain.class, "Executor of the entire process.");
	    	jd.addClass("RegexRecon", RegexReconMain.class, "Data pre-processor, utilizes regular expression to reconize words within a log line.");
	    	jd.addClass("Kmeans", KmeansMain.class, "Implementation of the k-means algorithm. Preferable to use in a pre-processed data file");
	    	jd.addClass("DisplayCentroids", DisplayCentroids.class, "Display and compare the centroids iteraction.");
	    	exitCode = jd.run(argv);
	    }
	    catch(Throwable e){
	    	e.printStackTrace();
	    }
	    System.exit(exitCode);
	}
}
