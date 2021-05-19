package kmeans;

import java.io.IOException;
//import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import resources.Resources;
import resources.Setup;

public class Kmeans {	
	public static class KMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			List<List<Double>> centroids = Resources.readList(conf, Double.class, Setup.CENTROID, conf.get(Setup.CENTROID_CUR_PATH));
			StringTokenizer st = new StringTokenizer(line.toString(),"\t");
			@SuppressWarnings("unused")
			String ip = st.nextToken();
			String values = Resources.normalize6(conf,conf.get(Setup.JOB_PATH),st.nextToken());
//			List<Double> distances = new ArrayList<>();
			int k = 0;
			int minDistIndex = k;
			Double minDist = 0.0;
			for(List<Double> centroid : centroids) {
				Double d = 0.0;
				StringTokenizer st1 = new StringTokenizer(values,";");
				int i=0;
				while(st1.hasMoreTokens()) {
					d += Math.pow(Double.parseDouble(st1.nextToken())-centroid.get(i++),2);
				}
				d = Math.sqrt(d);
				 if(k == 0 || d < minDist) {
					minDistIndex = k;
					minDist = d;
				}
				k++;
//				distances.add(Math.sqrt(d));
			}
			/*int k = 0;
			int minDistIndex = k;
			Double minDist = distance.get(k);
			for(Double d : distances) {
				if(d < minDist) {
					minDistIndex = k;
					minDist = d;
				}
				k++;
			}*/
			context.write(new Text(String.valueOf(minDistIndex)), new Text(/*ip+";"+*/values));
		}
	}
	public static class KReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
//			List<Double> newCentroid = listInit(Integer.parseInt(context.getConfiguration().get(VarSet.D_PARAM_SIZE)));
			Configuration conf = context.getConfiguration();
			List<List<Double>> newCentroids = Resources.readList(conf, Double.class, Setup.NEW_CENTROID, conf.get(Setup.CENTROID_CUR_PATH)); //Integer.parseInt(key.toString())
//			List<Text> ip = new ArrayList<>();
			List<Double> newCentroid = Resources.listInitZero(Integer.parseInt(conf.get(Setup.D_PARAM_SIZE)));
			for(Text t : lines) {
				StringTokenizer st = new StringTokenizer(t.toString(),";");
//				ip.add(new Text(st.nextToken()));
//				String ip = st.nextToken();
				int i = 0;
				while(st.hasMoreTokens()){
					Double d = newCentroid.get(i);
					newCentroid.set(i, d + Double.parseDouble(st.nextToken()));
					i++;
				}
//				context.write(key, new Text(t));
			}
//			int ttreq = conf.getInt(Setup.N_TOTAL_REQUESTS,1000);
			List<Double> variables = Resources.loadVar6(conf);
			String line = "";
			for(int j=0; j<Integer.parseInt(conf.get(Setup.D_PARAM_SIZE));j++) {
				if(variables.get(j) == 0.0) newCentroid.set(j,0.0);
				else newCentroid.set(j,Resources.decScale(newCentroid.get(j)/variables.get(j),Integer.parseInt(conf.get(Setup.USCALE))));
				line += newCentroid.get(j)+";";
			}
			newCentroids.set(Integer.parseInt(key.toString()), newCentroid);
			Resources.writeList(conf, newCentroids, Setup.NEW_CENTROID,conf.get(Setup.CENTROID_CUR_PATH));
			context.write(key, new Text(line));
		}
	}
}
