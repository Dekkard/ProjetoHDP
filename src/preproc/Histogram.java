package preproc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import resources.Resources;
import resources.Setup;

public class Histogram {
	public static class NormMaxMapper extends Mapper<Object, Text, Text, Text>{		
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(line.toString(),"\t;");
			@SuppressWarnings("unused")
			String ip = st.nextToken();
			List<Double> listVar = Resources.loadVar6(context.getConfiguration());
			context.write(new Text("Req"),	new Text(String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(0))));
			context.write(new Text("Sec"),	new Text(String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(1))));
			context.write(new Text("Get"),	new Text(String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(2))));
			context.write(new Text("Put"),	new Text(String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(3))));
			context.write(new Text("Post"),	new Text(String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(4))));
			context.write(new Text("Del"),	new Text(String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(5))));
//			context.write(new Text(ip),	new Text(String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(0))
//				+";"+String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(1))
//				+";"+String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(2))
//				+";"+String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(3))
//				+";"+String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(4))
//				+";"+String.valueOf(Double.parseDouble(st.nextToken())/listVar.get(5))+";"));
		}
	}
	public static class NormMaxReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<Integer> listBin = new ArrayList<>();
			int i = 0;
			while(i++<context.getConfiguration().getInt(Setup.HIST_BIN_DIV,10)) {
				listBin.add(0);
			}
			for(Text t : values) {
				Double v = Double.valueOf(t.toString());
				Double binDiv = 1.0/context.getConfiguration().getInt(Setup.HIST_BIN_DIV,10);
				Double bin = binDiv;
				i=0;
				while(bin<1.0) {
					if(v > bin-binDiv && v < bin) {
						listBin.set(i,listBin.get(i)+1);
						break;
					}
					bin += binDiv;
					i++;
				}
			}
			i=0;
			for(Integer bin_val : listBin) {
				context.write(new Text(key.toString()+"_"+String.valueOf(i)), new Text(String.valueOf(bin_val)));
				i++;
			}
		}
	}
	public static class NormGauMapper extends Mapper<Object, Text, Text, Text>{		
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(line.toString(),"\t;");
			@SuppressWarnings("unused")
			String ip = st.nextToken();
			Configuration conf = context.getConfiguration();
//			Double ttr = Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), Setup.N_TOTAL_REQUESTS);
			context.write(new Text("requests"),	new Text(String.valueOf(Math.abs(Double.parseDouble(st.nextToken())-
					((Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "sum_requests")/
							(Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "total_requests"))))));
			context.write(new Text("session"),	new Text(String.valueOf(Math.abs(Double.parseDouble(st.nextToken())-
					((Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "sum_session")/
							(Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "total_session"))))));
			context.write(new Text("get"),	new Text(String.valueOf(Math.abs(Double.parseDouble(st.nextToken())-
					((Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "sum_get")/
							(Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "total_get"))))));
			context.write(new Text("put"),	new Text(String.valueOf(Math.abs(Double.parseDouble(st.nextToken())-
					((Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "sum_put")/
							(Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "total_put"))))));
			context.write(new Text("post"),	new Text(String.valueOf(Math.abs(Double.parseDouble(st.nextToken())-
					((Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "sum_post")/
							(Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "total_post"))))));
			context.write(new Text("del"),	new Text(String.valueOf(Math.abs(Double.parseDouble(st.nextToken())-
					((Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "sum_del")/
							(Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "total_del"))))));
		}
	}
	public static class NormGauReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Double variance = 0.0;
			List<String> values = new ArrayList<>();
			for(Text t : lines) {
				variance += Math.pow(Double.valueOf(t.toString()),2);
				values.add(t.toString());
			}
			variance = Math.sqrt(variance/(Double)Resources.readFileVar(conf,Double.class, conf.get(Setup.JOB_PATH), "total_"+key.toString()));
			Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "variance_"+key.toString(), variance);
			List<Integer> listBin = new ArrayList<>();
			int i = 0;
			while(i++<context.getConfiguration().getInt(Setup.HIST_BIN_DIV,10)) {
				listBin.add(0);
			}
			for(String t : values) {
				Double v = Double.valueOf(t)/variance;
				Double binDiv = 1.0/context.getConfiguration().getInt(Setup.HIST_BIN_DIV,10);
				Double bin = binDiv;
				i=0;
				while(i<context.getConfiguration().getInt(Setup.HIST_BIN_DIV,10)) {
					if(v > bin-binDiv && v < bin) {
						listBin.set(i,listBin.get(i)+1);
						break;
					}
					bin += binDiv;
					i++;
				}
			}
			i=0;
			for(Integer bin_val : listBin) {
				context.write(new Text(key.toString()+"_"+(++i)), new Text(String.valueOf(bin_val)));
			}
		}
	}
}