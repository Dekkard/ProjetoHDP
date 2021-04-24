package kmeans;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

@SuppressWarnings ("unused")
public class Kmeans {
	/*Tamanho do Cluster K*/
	protected static Integer K_CLUSTER_SIZE = 5;
	/*Quantidade de parâmetros N*/
	protected static Integer D_PARAM_SIZE = 6;
	protected static Integer USCALE = 4;
	protected static Double ERROR_MARGIN = 0.0005;
	protected static List<List<DoubleWritable>> CENTROIDS;
	protected static List<List<DoubleWritable>> NEW_CENTROIDS;
	
	protected static Double decScale(Double number, int scale) {
		return new BigDecimal(number).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
	}
	protected static Double decScale(String number, int scale) {
		return new BigDecimal(number).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
	}
	
	protected static void printDoubleList(List<List<DoubleWritable>> list) {
		for(List<DoubleWritable> ls : list) {
			for(DoubleWritable l : ls) {
				System.out.println(l);
			}
			System.out.println("");
		}
	}
	
	public static class KMapper extends Mapper<Object, Text, Text, Text> {
		private static List<List<DoubleWritable>> centroidInit(Integer k, Integer n){
			List<List<DoubleWritable>> cs = new ArrayList<>();
			for(int i = 0; i < k; i++) {
				List<DoubleWritable> c = new ArrayList<DoubleWritable>();
				while(c.size() < n) {
					c.add(new DoubleWritable(decScale((1.0/(k+1.0))*(i+1.0),USCALE)));
				}
				cs.add(c);
			}
			return cs;
		}
		private static void centroidWrite(List<List<DoubleWritable>> centroids, Context context) throws IOException, InterruptedException {
			for(List<DoubleWritable> cs : centroids) {
				String t = "";
				for(DoubleWritable c : cs) {
					t += c.toString()+";";
				}
				context.write(new Text(""), new Text(t));
			}
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(CENTROIDS.size() == 0) {
				CENTROIDS = centroidInit(K_CLUSTER_SIZE,D_PARAM_SIZE);
			}
			StringTokenizer st = new StringTokenizer(value.toString());
			String ip = st.nextToken();
			context.write(new Text("ip"), new Text(ip+";"+st.nextToken()));
//			printDoubleList(centroids);
//			centroidWrite(centroids, context);
		}
		
	}
	
	public static class KReducer extends Reducer<Text,IntWritable,Text,Text> {
		private static List<List<List<DoubleWritable>>> centroidListInit(int k){
			List<List<List<DoubleWritable>>> centroid = new ArrayList<>();
			while(centroid.size() < k) {
				List<List<DoubleWritable>> c = new ArrayList<>();
				centroid.add(c);
			}
			return centroid;
		}
		private static List<List<DoubleWritable>> doubleListInit(int k, int d){
			List<List<DoubleWritable>> list = new ArrayList<>();
			while(list.size() < k) {
				List<DoubleWritable> l = new ArrayList<>();
				while(l.size() < d) {
					l.add(new DoubleWritable(0.0));
				}
				list.add(l);
			}
			return list;
		}
		private static List<List<DoubleWritable>> newCentroidCalc(List<List<List<DoubleWritable>>> centroidList){
			List<List<DoubleWritable>> newCentroid = doubleListInit(K_CLUSTER_SIZE,D_PARAM_SIZE);
			for(List<List<DoubleWritable>> centroid : centroidList){
				int i = 0;
				for(List<DoubleWritable> x : centroid) {
					int j = 0;
					for(DoubleWritable value : x) {
						double d = newCentroid.get(i).get(j).get();
						newCentroid.get(i).get(j).set(d + value.get());
						j++;
					}
				}
				for(int k=0; k<D_PARAM_SIZE;k++) {
					newCentroid.get(i).get(k).set(decScale(1.0/newCentroid.get(i).get(k).get(),USCALE));
				}
				i++;
			}
			return newCentroid;
		}
		private static List<List<DoubleWritable>> distanceCalc(List<List<DoubleWritable>> centroid, List<List<DoubleWritable>> values){
			List<List<List<DoubleWritable>>> centroidList = centroidListInit(K_CLUSTER_SIZE);
//			List<List<DoubleWritable>> newCentroids = doubleListInit(K_CLUSTER_SIZE,D_PARAM_SIZE);
			for(List<DoubleWritable> x : values) {
				List<DoubleWritable> distance = new ArrayList<>();
				for(List<DoubleWritable> c : centroid) {
					int j = 0;
					Double d = 0.0;
					for(int i=0;i<c.size();i++) {
						d += Math.pow( x.get(i).get() - c.get(i).get() , 2);
					}
//					distance.get(j++).set(Math.sqrt(d));
					distance.add(new DoubleWritable(Math.sqrt(d)));
				}
				int minDistIndex = 0;
				Double minDist = 0.0;
				int k = 0;
				for(DoubleWritable d : distance) {
					if(minDistIndex == 0 || minDist < d.get()) {
						minDistIndex = k;
						minDist = d.get();
					}
					k++;
				}
				centroidList.get(minDistIndex).add(x);
			}
			List<List<DoubleWritable>> newCentroids = newCentroidCalc(centroidList);
			return newCentroids;
		}
		public static boolean compareLists(List<List<DoubleWritable>> list1, List<List<DoubleWritable>> list2, Double cp) {
			for(int i=0;i<list1.size();i++) {
				for(int j=0;j<list1.get(i).size();j++) {
					if(Math.abs(list1.get(i).get(j).get()-list2.get(i).get(j).get()) > cp) {
						return true;
					}
				}
			}
			return false;
		}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<List<DoubleWritable>> X = new ArrayList<>();
			List<Text> ip = new ArrayList<>();
			int i = 0;
			for(Text t : values) {
				StringTokenizer st = new StringTokenizer(t.toString());
				ip.add(new Text(st.nextToken()));
				while(st.hasMoreTokens()){
					X.get(i).add(new DoubleWritable(decScale(st.nextToken(),USCALE)));
				}
			}
			NEW_CENTROIDS = distanceCalc(CENTROIDS,X);
			if(compareLists(CENTROIDS,NEW_CENTROIDS,ERROR_MARGIN)) {
				CENTROIDS = NEW_CENTROIDS;
				NEW_CENTROIDS = distanceCalc(CENTROIDS,X);
			} else {
				CENTROIDS = NEW_CENTROIDS;
				for(List<DoubleWritable> centroid : CENTROIDS) {
					String t = "";
					for(DoubleWritable c : centroid) {
						t += c.toString()+";";
					}
					context.write(new Text("Centroid"), new Text(t));
				}
			}
		}
	}
}
