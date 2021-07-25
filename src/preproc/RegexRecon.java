package preproc;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

import model.WebLog;
import resources.Resources;
import resources.Setup;

public class RegexRecon {	
	public static class RegexMapper extends Mapper<Object, Text, Text, Text>{		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			WebLog wl = new WebLog(value.toString());
			if(!wl.getIp().isEmpty()) {
				context.write(new Text(wl.getIp()), new Text(1+";"+wl.getSeconds()+";"+wl.getMethod()+";"));
				context.write(new Text(Setup.N_TOTAL), new Text("1"));
			}
		}
	}
	public static class RegexReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(key.toString().equals(Setup.N_TOTAL)) {
				Configuration conf = context.getConfiguration();
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), Setup.N_TOTAL, Iterables.size(values) );
			} else if(key.toString().matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
				int uReq = 0; 
				int max = 0, min = 0;
				int get = 0, put = 0, post = 0, del = 0;
				boolean first = true;
				for(Text t : values) {
					StringTokenizer st = new StringTokenizer(t.toString(),";");
					int v;
					String m;
					v = Integer.parseInt(st.nextToken());
					uReq += v;
					v = Integer.parseInt(st.nextToken());
					if(first || v < min) {
						min = v;
					}
					if(first || v > max) {
						max = v;
					}
					if(first) first = false;
					m = st.nextToken();
					if(m.matches(".*(GET).*")) {
						get++;
					} else if(m.matches(".*(PUT).*")) {
						put++;
					} else if(m.matches(".*(POST).*")) {
						post++;
					} else if(m.matches(".*(DELETE).*")) {
						del++;
					}
				}
				context.write(key, new Text(uReq+";"+(max - min)+";"+get+";"+put+";"+post+";"+del+";"));
			}
		}
	}
	public static class MidMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString(),"\t ;");
			@SuppressWarnings("unused")
			String ip = st.nextToken();
			context.write(new Text(Setup.MAX_REQ), 	new Text(st.nextToken()));
			context.write(new Text(Setup.MAX_SEC),	new Text(st.nextToken()));
			context.write(new Text(Setup.MAX_GET),	new Text(st.nextToken()));
			context.write(new Text(Setup.MAX_PUT),	new Text(st.nextToken()));
			context.write(new Text(Setup.MAX_POST),	new Text(st.nextToken()));
			context.write(new Text(Setup.MAX_DEL),	new Text(st.nextToken()));
		}
	}
	public static class MidReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int max_val = 0;
			int sum_val = 0;
			int total_val = 0;
			boolean f = true;
			for(Text t : values) {
				int val = Integer.parseInt(t.toString());
				sum_val += val;
				total_val++;
				if(f) { 
					max_val = val;
					f = false;
				} else if(val>max_val) max_val = val;
			}
			Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), key.toString(), max_val);
			Matcher m = Pattern.compile(".*(max_)(\\w+).*").matcher(key.toString());
			if(m.find()) {
//				context.write(new Text("sum_"+m.group(2)), new Text(String.valueOf(sum_val)));
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "sum_"+m.group(2), sum_val);
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "total_"+m.group(2), total_val);
			}
		}
	}
}
