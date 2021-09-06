package preproc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.exception.NotANumberException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import model.WebLog;
import resources.Resources;
import resources.Setup;

public class RegexRecon {
	public static class RegexMapper extends Mapper<Object, Text, Text, Text> {
//		List<Integer> bot_list;
		public static enum PREPOST {
			SETUP, CLEANUP
		}

		public void setup(Context context) {
//			bot_list = new ArrayList<>();
			context.getCounter(PREPOST.SETUP).increment(1);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Matcher mat = Pattern.compile(".*(bot).*", Pattern.CASE_INSENSITIVE).matcher(value.toString());
			if (!mat.find()) {
//				System.out.println("Begin line");
				Configuration c = context.getConfiguration();
				WebLog wl = new WebLog(value.toString(), c.get(Setup.WORD_WANTED), c.get(Setup.WORD_UNWANTED));
				if (!wl.getIp().isEmpty() && wl.getUrl() != null) {
					context.write(new Text(wl.getIp()),
							new Text(1 + ">next>" + wl.getSeconds() + ">next>" + wl.getCode() + ">next>" + wl.getBytes()
									+ ">next>" + wl.getMethod() + ">next>" + wl.getUrl()));
					context.write(new Text(Resources.urlPruner(wl.getUrl())), new Text("1"));
//					context.write(new Text("URL:"+wl.getIp()), new Text(wl.getUrl()));
//					context.write(new Text(Setup.N_TOTAL), new Text("1"));
				}
			} // else bot_list.add(1);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
//			context.write(new Text("Bots"), new Text(String.valueOf(bot_list.size())));
			context.getCounter(PREPOST.CLEANUP).increment(1);
		}
	}

	public static class RegexReducer extends Reducer<Text, Text, Text, Text> {
		/* Setup do escritor de multiplas saídas */
		private MultipleOutputs<Text, Text> mos; // Definir pelos tipos do par chave e valor
		List<Integer> ip_list;

//		List<Integer> session_list;
		protected void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
			ip_list = new ArrayList<>();
//			session_list = new ArrayList<>();
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.toString().matches(Resources.REGEX_IP)) {
				ip_list.add(1);
				int uReq = 0;
				long seconds;
				List<Long> sessions = new ArrayList<>();
				int get = 0, put = 0, post = 0, del = 0;
				Integer[] codes = { 0, 0, 0, 0, 0 };
				int ttl_bytes = 0;
				List<String> l1 = new ArrayList<>();
				List<Integer> l2 = new ArrayList<>();
				for (Text t : values) {
					String[] value = t.toString().split(">next>");
					int v;
					String m;
					v = Integer.parseInt(value[0]);
					uReq += v;
					seconds = Long.parseLong(value[1]);
					int l_size = sessions.size();
					if (l_size == 0) {
						sessions.add(seconds);
					} else {
						for (int i = 0; i < l_size; i++) {
							Long s = sessions.get(i);
							if (seconds <= s) {
								sessions.add(i, seconds);
								break;
							} else if (i == l_size - 1) {
								sessions.add(seconds);
								break;
							}
						}
					}
					v = Integer.parseInt(value[2]);
					int code = 100;
					for (int i = 0; i < codes.length; i++) {
						if (v >= code && v <= (code + 99)) {
							codes[i] += 1;
						}
						code += 100;
					}
					ttl_bytes += Integer.parseInt(value[3]);
					m = value[4];
					if (m.matches(".*(GET).*")) {
						get++;
					} else if (m.matches(".*(PUT).*")) {
						put++;
					} else if (m.matches(".*(POST).*")) {
						post++;
					} else if (m.matches(".*(DELETE).*")) {
						del++;
					}
					String url = value[5];
					if (l1.contains(url)) {
						int i = l1.indexOf(url);
						l2.set(i, l2.get(i) + 1);
					} else {
						l1.add(url);
						l2.add(1);
					}
				}
				long last_sec = 0;
				int session_sum = 0;
				int session_total = 1;
				int sessions_qtd = 1;
				Double sessions_median = 0.0;
				Integer session_time = 1;

				boolean newsession = true;
				for (Long sec : sessions) {
					long dif = 0;
					if (newsession) {
						last_sec = sec;
						newsession = false;
					} else {
						dif = (sec - last_sec) / 1000;
						if (dif >= 900) {
							sessions_qtd++;
							session_time += session_sum;
							sessions_median += session_sum / session_total;

							session_sum = 0;
							session_total = 1;
							newsession = true;
						} else {
							last_sec = sec;
							session_sum += dif;
							session_total++;
							newsession = false;
						}
					}
				}
				if (!newsession) {
					session_time += session_sum;
					try {
						sessions_median += session_sum / session_total;
					} catch (ArithmeticException e) {
						sessions_median = 0.0;
					}
				}
				String session_line = session_time + ";" + (sessions_median / sessions_qtd) + ";" + sessions_qtd;
				String codeString = codes[0] + ";" + codes[1] + ";" + codes[2] + ";" + codes[3] + ";" + codes[4];
				context.write(key, new Text(uReq + ";" + session_line + ";" + codeString + ";" + ttl_bytes + ";" + get
						+ ";" + put + ";" + post + ";" + del + ";"));
				String line = "";
				for (int i = 0; i < l1.size() - 1; i++) {
					line += l1.get(i) + ":QTD>" + l2.get(i) + ";NEXT:";
				}
				mos.write("URL", key, new Text(line));
			} else {
				mos.write("KnownURL", key, null);
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), Setup.N_TOTAL, ip_list.size());
		}
	}

	public static class UrlMapperParam extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("	");
			context.write(new Text(v[0]), new Text("1>" + v[1]));
		}
	}

	public static class UrlMapperParse extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("	");
			String urls = "2>";
			if (v.length > 1) {
				urls += v[1];
			}
			context.write(new Text(v[0]), new Text(urls));
		}
	}

	public static class UrlReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			String param_part = null, url_part = "";
			for (Text t : values) {
				String value = t.toString();
				Matcher mat = Pattern.compile("(1>)(.*)|(2>)(.*)").matcher(value);
				if (mat.find()) {
					if (mat.group(1) != null) {
						param_part = mat.group(2);
					} else if (mat.group(3) != null) {
						String v = mat.group(4);
						List<String> url_list = new ArrayList<>();
						List<Integer> qtd_list = new ArrayList<>();
						if (!v.isEmpty()) {
							String[] val = v.split(";NEXT:");
							for (String st : val) {
								String[] lurl = st.split(":QTD>");
								lurl[0] = Resources.urlPruner(lurl[0]);
								if (url_list.contains(lurl[0])) {
									int i = url_list.indexOf(lurl[0]);
									qtd_list.set(i, qtd_list.get(i) + Integer.parseInt(lurl[1]));
								} else {
									url_list.add(lurl[0]);
									qtd_list.add(Integer.parseInt(lurl[1]));
								}
							}
						}
						URI[] uris = context.getCacheFiles();
						for (URI uri : uris) {
							Path path = new Path(uri.getPath());
							if (path.getName().matches("(KnownURL-r-)(\\d+)")) {
								FSDataInputStream in = fs.open(path);
								BufferedReader br = new BufferedReader(new InputStreamReader(in));
								String known_url;
								while ((known_url = br.readLine()) != null) {
									if (url_list.contains(known_url)) {
										int i = url_list.indexOf(known_url);
										url_part += qtd_list.get(i) + ";";
									} else
										url_part += "0;";
								}
								br.close();
								in.close();
							}
						}
					}
				}
			}
			context.write(key, new Text(param_part + url_part));
		}
	}

	public static class NormMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString(), "\t ;");
			@SuppressWarnings("unused")
			String ip = st.nextToken();
			int i = 0;
			while (st.hasMoreTokens()) {
				String index = String.valueOf(++i);
				context.write(new Text("Param_" + "0".repeat(7 - index.length()) + index), new Text(st.nextToken()));
				context.write(new Text("param-var"), new Text("1"));
//				param_size.add(1);
			}
			context.write(new Text("param-var"), new Text("2"));
		}
	}

	public static class NormReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			if (key.toString().equals("param-var")) {
				Integer total_param = 0;
				Integer total_lines = 0;
				for (Text t : values) {
					if (t.toString().equals("1"))
						total_param++;
					else if (t.toString().equals("2"))
						total_lines++;
				}
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "param", total_param / total_lines);
			} else {
				Double max_val = 0.0;
				Double sum_val = 0.0;
				int total_val = 0;
				boolean f = true;
				List<Double> list = new ArrayList<>();
				for (Text t : values) {
					Double val = Double.parseDouble(t.toString());
					list.add(val);
					sum_val += val;
					total_val++;
					if (f) {
						max_val = val;
						f = false;
					} else if (val > max_val)
						max_val = val;
				}
				Double variance = 0.0;
				for (Double value : list) {
					try {
						variance += Math.pow(value - (sum_val / total_val), 2);
					} catch (NotANumberException | ArithmeticException e) {
						variance = 0.0;
					}
				}
				variance = Math.sqrt(variance);
				List<Integer> listBin = new ArrayList<>();
				Integer bin_div = conf.getInt(Setup.HIST_BIN_DIV, 10);
				int i = 0;
				while (i++ < bin_div) {
					listBin.add(0);
				}
				for (Double t : list) {
					Double v;
					if (variance == 0.0)
						v = 0.0;
					else {
						try {
							v = Resources.distr(t, sum_val, (double) total_val, variance, conf.get(Setup.DIST_METHOD));
						} catch (ArithmeticException e) {
							v = 0.0;
						}
						if (Double.isNaN(v))
							v = 0.0;
					}
					context.write(key, new Text(String.format("%.8f", v)));
					Double bin_size = 1.0 / bin_div;
					Double cur_bin = bin_size;
					i = 0;
					while (i < bin_div) {
						if (v >= cur_bin - bin_size && v < cur_bin) {
							Integer number = listBin.get(i);
							listBin.set(i, number + 1);
							break;
						}
						cur_bin += bin_size;
						i++;
					}
				}
				mos.write("VarianceMeta", key, new Text(sum_val + ";" + total_val + ";" + variance));
				i = 0;
				for (Integer bin_val : listBin) {
					String index = String.valueOf(++i);
					index = "0".repeat(String.valueOf(bin_div).length() - index.length()) + index;
					mos.write("Histogram", new Text(key.toString() + "-bin_" + index),
							new Text(bin_val.toString()));
				}
			}
		}
	}
}
