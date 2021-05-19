package preproc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import model.WebLog;
import resources.Resources;
import resources.Setup;

public class RegexRecon {	
	public static class RegexMapper extends Mapper<Object, Text, Text, Text>{		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			WebLog wl = new WebLog(value.toString());
			if(!wl.getIp().isEmpty()) {
				context.write(new Text(wl.getIp()), new Text(1+";"+wl.getSeconds()+";"+wl.getMethod()+";"));
			}
		}
	}
	public static class RegexReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
	public static class MidComparator extends WritableComparator {
		public MidComparator() {
			super(Text.class, true);
		}
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			Text val1 = (Text) wc1;
			Text val2 = (Text) wc2;
			return val1.toString().compareTo(val2.toString());
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
			boolean f = true;
			for(Text t : values) {
				int val = Integer.parseInt(t.toString());
				if(f) { 
					max_val = val;
					f = false;
				} else if(val>max_val) max_val = val;
			}
			Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), key.toString(), max_val);
		}
	}
}
