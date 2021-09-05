package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import resources.Resources;
import resources.Setup;

//@SuppressWarnings("all")
public class Kmeans {
	public static class KNormalizer extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
//			StringTokenizer st = new StringTokenizer(line.toString(),"\t");
//			String ip = st.nextToken();
			String[] vector = line.toString().split("	");
			String ip = vector[0];
			vector = vector[1].split(";");
			int i = 0;
			String newline = "";
			RemoteIterator<FileStatus> lfs = FileSystem.get(conf)
					.listStatusIterator(new Path(conf.get(Setup.JOB_PATH) + "/data.meta"));
			while (lfs.hasNext()) {
				Path p = lfs.next().getPath();
				if (p.getName().matches("(VarianceMeta-r-)(\\d+)")) {
					FileSystem fs = FileSystem.get(conf);
					FSDataInputStream in = fs.open(p);
					BufferedReader br = new BufferedReader(new InputStreamReader(in));
					String rl;
//			    	List<String> list = new ArrayList<>();
//					String[] vector = st.nextToken().split(";");
					while ((rl = br.readLine()) != null) {
//			        	list.add(known_url);
						String[] meta = rl.split("	");
						meta = meta[1].split(";");
						Double x = Double.parseDouble(vector[i++]);
						Double s = Double.parseDouble(meta[0]);
						Double t = Double.parseDouble(meta[1]);
						Double d = Double.parseDouble(meta[2]);
						if (d == 0.0)
							newline += "0.0;";
						else {
							try {
								Double v = Math.abs((x - (s / t)) / d);
								newline += (Double.isNaN(v) ? 0.0 : v) + ";";
							} catch (ArithmeticException e) {
								newline += "0.0;";
							}
						}
					}
					br.close();
					in.close();
//					List<String> kurl = Resources.readStringList(conf,path);
//					KnownURL.addAll(kurl);
				}
			}
			context.write(new Text(ip), new Text(newline));
		}
	}

	public static class KCentroidInnitMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Integer kluster = conf.getInt(Setup.K_CLUSTER_SIZE, 5);
			Integer bin_size = Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), "hist_bin");
			String[] param = line.toString().split("	");
			String p = param[0];
			param = param[1].split(";");
			Double total = Double.parseDouble(param[1]);
			Double part = total / kluster;
//        	Double part = Resources.decScale(total/kluster, 8);
			Path path = new Path(conf.get(Setup.JOB_PATH) + "/data.meta");
			RemoteIterator<FileStatus> lfs2 = fs.listStatusIterator(path);
			while (lfs2.hasNext()) {
				Path p2 = lfs2.next().getPath();
				if (p2.getName().matches("(Histogram-r-)(\\d+)")) {
					FSDataInputStream in2 = fs.open(p2);
					BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
					String rl2;
//					Double qtd_ = 0.0;
					Double qtd = 0.0;
					Double cur_bin = 0.0;
					int cur_div = 0;
					while ((rl2 = br2.readLine()) != null && cur_bin <= 1) {
						String[] bin = rl2.split("	");
						if (bin[0].split("-bin_(\\d+)")[0].equals(p)) {
							System.out.println(bin[0]);
							Integer cur_bin_val = Integer.parseInt(bin[1]);
//							qtd_ += cur_bin_val;
							qtd += cur_bin_val;
							if (qtd >= (part * (cur_div + 1)) || qtd - (part * (cur_div + 1)) <= 1E-4) {
								int div = (int) Math.floor(qtd / part);
//			        			div += cur_div;
//						        qtd_ac += qtd;
//			        			qtd_ = qtd_%part;
//			        			qtd_ = Resources.decScale(qtd_,8);
								for (int d = cur_div; d < div; d++) {
									double dnum = (cur_bin) + ((1.0 / bin_size * d) * (part / qtd));
									String index = String.valueOf(d + 1);
									index = "0".repeat(String.valueOf(kluster).length() - index.length()) + index;
									context.write(new Text(String.valueOf(index)), new Text(p + ":" + dnum));
									System.out.println("Bin at.:" + cur_bin + "| Tam. Bin:" + bin_size + "| Total:"
											+ total + "| Parte:" + part + "| Qtd ac.:" + qtd + "| Div. at.:" + cur_div
											+ "| Div ac.:" + div + "| Div. it.:" + d);
								}
								cur_div = div;
							} else
								System.out.println("Bin at.:" + cur_bin + "| Tam. Bin:" + bin_size + "| Total:" + total
										+ "| Parte:" + part + "| Qtd ac.:" + qtd + "| Div. at.:" + cur_div);
							cur_bin += 1.0 / bin_size;
							if (qtd >= total)
								break;
						}
//		        		System.out.println(bin[0]+"\tTotal:"+total+"\tQtd: "+qtd+"\tacumulado: "+qtd_ac+"\tparte:"+part+"\tparam:"+p+"\tbin:"+bin_size+"\tatual:"+cur_bin+"\tdiv: "+cur_div);
					}
				}
			}
		}
	}

	public static class KCentroidsInnitReducer extends Reducer<Text, Text, Text, Text> {
		@SuppressWarnings("all")
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
//			Integer bin_size = Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), "hist_bin");
			Integer param = Resources.readFileVar(conf, Integer.class, conf.get(Setup.JOB_PATH), "param");
			String[] centroid = new String[param];
			for (Text l : lines) {
				String[] c = l.toString().split(":");
				centroid[Integer.parseInt(c[0].split("Param_")[1]) - 1] = c[1];
			}
			String line = "";
			for (String c : centroid) {
				line += c + ";";
			}
//			context.write(key, new Text(line));
			mos.write("Centroids", key, new Text(line));
		}
	}

	public static class KLoadCentroidMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object obj, Text line, Context context) throws IOException, InterruptedException {
			String[] centroid = line.toString().split("	");
			context.write(new Text(centroid[0]), new Text("centroids>" + centroid[1]));
		}
	}

	public static class KMapper extends Mapper<Object, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		List<String> centroid_old = new ArrayList<>();

		public void setup(Context context) throws IOException {
			mos = new MultipleOutputs<Text, Text>(context);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			URI[] uri_list = context.getCacheFiles();
			for (URI uri : uri_list) {
				Path p = new Path(uri.getPath());
				FSDataInputStream in = fs.open(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String rl;
				while ((rl = br.readLine()) != null) {
					centroid_old.add(rl);
				}
				br.close();
				in.close();
			}
		}

		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
//			FileSystem fs = FileSystem.get(conf);
//			List<List<Double>> centroids = Resources.readList(conf, Double.class, Setup.CENTROID, conf.get(Setup.CENTROID_CUR_PATH));
//			StringTokenizer st = new StringTokenizer(line.toString(),"\t");
//			String ip = st.nextToken();
			String[] values = line.toString().split("	");
			String ip = values[0];
			String newline = values[1];
			values = newline.split(";");
//			List<Double> values = Resources.normalize(conf,conf.get(Setup.JOB_PATH)+"/data.vector",st.nextToken()); // String values
//			List<Double> distances = new ArrayList<>();
			int k = 1;
			int minDistIndex = k;
			Double minDist = 0.0;
//			Path c_path = new Path(conf.get(Setup.CENTROID_CUR_PATH));
//			RemoteIterator<FileStatus> lfs = fs.listStatusIterator(c_path);
//			while (lfs.hasNext()) {
//				Path path = lfs.next().getPath();
//				if (path.getName().matches("(Centroids-r-)(\\d+)")) {
//					FSDataInputStream in = fs.open(path);
//					BufferedReader br = new BufferedReader(new InputStreamReader(in));
//					String rl;
//					while ((rl = br.readLine()) != null) {
			for (String co : centroid_old) {
//						String[] centroid = rl.split("	");
				String[] centroid = co.split("	");
				centroid = centroid[1].split(";");
				Double d = 0.0;
				// StringTokenizer st1 = new StringTokenizer(values,";");
				int i = 0;
				for (String v : values) { // st1.hasMoreTokens() Double.parseDouble(st1.nextToken())
					d += Math.pow(Double.parseDouble(v) - Double.parseDouble(centroid[i++]), 2);
				}
				d = Math.sqrt(d);
				if (k == 1 || d < minDist) {
					minDistIndex = k;
					minDist = d;
				}
				k++;
				// distances.add(Math.sqrt(d));
			}
//				}
//			}
//			for(List<Double> centroid : centroids) {}
			/*
			 * int k = 0; int minDistIndex = k; Double minDist = distance.get(k); for(Double
			 * d : distances) { if(d < minDist) { minDistIndex = k; minDist = d; } k++; }
			 */
			int kluster = conf.getInt(Setup.K_CLUSTER_SIZE, 10);
			String named_index = String.valueOf(minDistIndex);
			named_index = "0".repeat(String.valueOf(kluster).length() - named_index.length()) + named_index;
			context.write(new Text(named_index), new Text("newcentroids>" + newline));
			mos.write("GroupList" + named_index, new Text(ip), null);
//			context.write(new Text("GroupList"+named_index), new Text(ip));
		}
	}

	public static class KReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			/*
			 * if(key.toString().matches("GroupList(\\d+)")) { for(Text t : lines)
			 * mos.write(key.toString(), t, null); } else
			 * if(key.toString().matches("(\\d+)")){
			 */
			Configuration conf = context.getConfiguration();
//			List<Double> newCentroid = listInit(Integer.parseInt(context.getConfiguration().getInt(VarSet.D_PARAM_SIZE,15)));
//			List<List<Double>> newCentroids = Resources.readList(conf, Double.class, Setup.NEW_CENTROID, conf.get(Setup.CENTROID_CUR_PATH)); //Integer.parseInt(key.toString())
//			List<Text> ip = new ArrayList<>();
//			List<Double> newCentroid = Resources.listInitZero(Integer.parseInt(conf.getInt(Setup.D_PARAM_SIZE,15)));				
			String centroidline = null;
			String[] centroid = null;
			List<Double> newCentroid = new ArrayList<>();
			Integer num_list = 0;
			for (Text t : lines) {
//				StringTokenizer st = new StringTokenizer(t.toString(),"\t");
//				String ip = st.nextToken();
//				String[] value = st.nextToken().split(";");
				String[] value = t.toString().split(">");
				String type = value[0];
				if (type.equals("centroids")) {
					centroidline = value[1];
					centroid = centroidline.split(";");
				} else {
					value = value[1].split(";");
					// ip.add(new Text(st.nextToken()));
					// List<Double> value =
					// Resources.normalize6(conf,conf.get(Setup.JOB_PATH),st.nextToken());
					int i = 0;
					// Double d = 0.0;
					for (String v : value) {
						// Double d = newCentroid.get(i);
						Double d = Double.parseDouble(v);
						// newCentroid.set(i, d + Double.valueOf(v));
						// //Double.parseDouble(st.nextToken())
						try {
							newCentroid.set(i, d + newCentroid.get(i));
						} catch (IndexOutOfBoundsException e) {
							newCentroid.add(d);
						}
						i++;
					}
					num_list++;
				}
//				context.write(key, new Text(t));
			}
//			int ttreq = conf.getInt(Setup.N_TOTAL_REQUESTS,1000);
//			List<Double> variables = Resources.loadVar6(conf);
			String compare_line = "0";
			if (num_list > 0) {
				String line = "";
				for (int j = 0; j < conf.getInt(Setup.D_PARAM_SIZE, 15); j++) {
//					if(variables.get(j) == 0.0) newCentroid.set(j,0.0);else 
					Double v;
					try {
						v = newCentroid.get(j) / num_list;/* /variables.get(j) */
					} catch (ArithmeticException e) {
						v = 0.0;
					}
//					newCentroid.set(j,Resources.decScale((Double.isNaN(v)?0.0:v),Integer.parseInt(conf.get(Setup.USCALE))));
					line += Resources.decScale((Double.isNaN(v) ? 0.0 : v), Integer.parseInt(conf.get(Setup.USCALE)))
							+ ";";

					Double dif = Math.abs(Double.parseDouble(centroid[j]) - v);
					if (dif > context.getConfiguration().getDouble(Setup.ERROR_MARGIN, 5E-15)) {
						compare_line = "1";
					}
				}
//				newCentroids.set(Integer.parseInt(key.toString()), newCentroid);
//				Resources.writeList(conf, newCentroids, Setup.NEW_CENTROID,conf.get(Setup.CENTROID_CUR_PATH));
//				context.write(key, new Text(line));
				mos.write("Centroids", key, new Text(line));
			} else {
				compare_line = "1";
				mos.write("Centroids", key, new Text(centroidline));
			}
			mos.write("Compare", key, new Text(compare_line));
//			}
		}
	}

	public static class KCentroidComparatorMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object obj, Text line, Context context) throws IOException, InterruptedException {
			String[] centroid = line.toString().split("	");
			context.write(new Text(centroid[0]), new Text("centroids>" + centroid[1]));
		}
	}

	public static class KNewCentroidComparatorMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			String[] newcentroid = line.toString().split("	");
			context.write(new Text(newcentroid[0]), new Text("newcentroids>" + newcentroid[1]));
		}
	}

	public static class KCentroidComparatorReducer extends Reducer<Text, Text, Text, Text> {
		@SuppressWarnings("all")
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			String[] centroids = null;
			String[] newcentroids = null;
			String newline1 = null;
			String newline2 = null;
			String newcentroids_line = "";
			String compare_line = "";
			for (Text t : lines) {
				String[] comp = t.toString().split(">");
				if (comp[0].equals("centroids")) {
					centroids = comp[1].split(";");
					newline1 = comp[1];
				} else if (comp[0].equals("newcentroids")) {
					newcentroids = comp[1].split(";");
					newline2 = comp[1];
				}
			}
			if (newcentroids != null) {
				newcentroids_line = newline2;
				compare_line = "0";
				for (int i = 0; i < centroids.length - 1; i++) {
					Double dif = Math.abs(Double.parseDouble(centroids[i]) - Double.parseDouble(newcentroids[i]));
					if (dif > context.getConfiguration().getDouble(Setup.ERROR_MARGIN, 5E-15)) {
						compare_line = "1";
						break;
					}
				}
			} else {
				newcentroids_line = newline1;
				compare_line = "1";
//				for(int i=0;i<centroids.length-1;i++) {}
			}
//			context.write(key, new Text(line));
			mos.write("Centroids", key, new Text(newcentroids_line));
			mos.write("Compare", key, new Text(compare_line));
		}
	}
}
