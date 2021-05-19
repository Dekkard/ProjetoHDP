package jobs;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import resources.Resources;
import resources.Setup;

public class DisplayCentroids {
	public static void main(String[] args) throws IOException {
		if(args.length > 1) {
			System.err.println("Compares only one pair of centroid and new centroid, just name the folder only.");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		List<List<Double>> centroids = Resources.readList(conf, Double.class, Setup.CENTROID,args[0]);
		List<List<Double>> new_centroids = Resources.readList(conf, Double.class, Setup.NEW_CENTROID,args[0]);
		System.out.println(Setup.CENTROID);
		Resources.printList(centroids,"|",true);
		System.out.println(Setup.NEW_CENTROID);
		Resources.printList(new_centroids,"|",true);
		System.out.println("\nDiference:");
		for(int i=0;i<centroids.size();i++) {
//			System.out.print(i+": ");
			for(int j=0;j<centroids.get(i).size();j++) {
//				System.out.print(j+" ");
				System.out.print(centroids.get(i).get(j)-new_centroids.get(i).get(j)+"\t");
			}
			System.out.println();
		}
		fs.close();
	}
}
