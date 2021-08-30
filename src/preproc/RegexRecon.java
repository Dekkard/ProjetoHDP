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

import com.google.common.collect.Iterables;

import model.WebLog;
import resources.Resources;
import resources.Setup;

public class RegexRecon {	
	public static class RegexMapper extends Mapper<Object, Text, Text, Text>{		
//		List<Integer> bot_list;
//		public void setup(Context context) {
//			bot_list = new ArrayList<>();
//		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Matcher mat = Pattern.compile(".*(bot).*",Pattern.CASE_INSENSITIVE).matcher(value.toString());
			if(!mat.find()) {
//				System.out.println("Begin line");
				Configuration c = context.getConfiguration();
				WebLog wl = new WebLog(value.toString(),c.get(Setup.WORD_WANTED),c.get(Setup.WORD_UNWANTED));
				if(!wl.getIp().isEmpty() && wl.getUrl()!=null) {
					context.write(new Text(wl.getIp()), new Text(1+">next>"+wl.getSeconds()+">next>"+wl.getCode()+">next>"+wl.getBytes()+">next>"+wl.getMethod()+">next>"+wl.getUrl()));
					context.write(new Text(Resources.urlPruner(wl.getUrl())), new Text("1"));
//					context.write(new Text("URL:"+wl.getIp()), new Text(wl.getUrl()));
					context.write(new Text(Setup.N_TOTAL), new Text("1"));
				}
			} //else bot_list.add(1);
		}
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			context.write(new Text("Bots"), new Text(String.valueOf(bot_list.size())));
//		}
	}
	
	public static class RegexReducer extends Reducer<Text,Text,Text,Text> {
		/*Setup do escritor de multiplas saídas*/
		private MultipleOutputs<Text, Text> mos; //Definir pelos tipos do par chave e valor
//		List<Integer> ip_list;
//		List<Integer> session_list;
		protected void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
//			ip_list = new ArrayList<>();
//			session_list = new ArrayList<>();
		}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/*class URL { String url;Integer qtd;public URL(String url, Integer qtd) { this.url = url; this.qtd = qtd; }public String toString() { return url+":"+qtd; }}*/
			if(key.toString().equals(Setup.N_TOTAL)) {
				Configuration conf = context.getConfiguration();
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH),Setup.N_TOTAL,Iterables.size(values));
			} /*else if(key.toString().equals("Bots")) {
				int bots_qtd = 0;
				for(Text t : values) {
					bots_qtd += Integer.parseInt(t.toString());
				}
				mos.write("Meta", key, new Text(String.valueOf(bots_qtd)));
			}*/ else if(key.toString().matches(Resources.REGEX_IP)) {
//				ip_list.add(1);
				int uReq = 0;
				long seconds;
				List<Long> sessions = new ArrayList<>();
//				long  max = 0, min = 0;
//				boolean first = true;
				int get = 0, put = 0, post = 0, del = 0;
				Integer[] codes = {0,0,0,0,0};
				int ttl_bytes = 0;
				List<String> l1 = new ArrayList<>();
				List<Integer> l2 = new ArrayList<>();
				for(Text t : values) {
//					StringTokenizer st = new StringTokenizer(t.toString(),";");
					String[] value = t.toString().split(">next>");
					int v;
					String m;
					v = Integer.parseInt(value[0]); //Integer.parseInt(st.nextToken());
					uReq += v;
					seconds = Long.parseLong(value[1]); //Long.parseLong(st.nextToken());
					/*sessions_total++; sessions_median += seconds; if(first || seconds < min) min = seconds; if(first || seconds > max) max = seconds;	if(first) first = false;*/
					int l_size = sessions.size();
					if(l_size==0) {
						sessions.add(seconds);
					} else {
						for(int i=0;i<l_size;i++) {
							Long s = sessions.get(i);
							if(seconds<=s) {
								sessions.add(i, seconds);
								break;
							} else if(i==l_size-1) {
								sessions.add(seconds);
								break;
							}
						}
					}
					v = Integer.parseInt(value[2]); //Integer.parseInt(st.nextToken());
					int code = 100;
					for(Integer c : codes) {
						if(v>code && v<(code+99)) {
							c += 1;
						}
						code += 100;
					}
					ttl_bytes += Integer.parseInt(value[3]); //Integer.parseInt(st.nextToken());
					m = value[4]; //st.nextToken();
					if(m.matches(".*(GET).*")) {
						get++;
					} else if(m.matches(".*(PUT).*")) {
						put++;
					} else if(m.matches(".*(POST).*")) {
						post++;
					} else if(m.matches(".*(DELETE).*")) {
						del++;
					}
					String url = value[5];
					if(l1.contains(url)) {
						int i = l1.indexOf(url);
						l2.set(i,l2.get(i)+1);
					} else {
						l1.add(url);
						l2.add(1);
					}
				}
				long last_sec = 0;
				int session_sum = 0;
				int session_total = 1;
				int sessions_qtd = 1;
//				session_list.add(1);
				Double sessions_median = 0.0;
				Integer session_time = 1;
//				int sessions_total = 0;
				
				boolean newsession = true;
				for(Long sec : sessions) {
					long dif = 0;
					if(newsession) {
						last_sec = sec;
						newsession = false;
					} else {
						dif = (sec-last_sec)/1000;
						if(dif>=900) {
							sessions_qtd++;
//							session_list.add(1);
							session_time += session_sum;
//							sessions_total += session_total;
							sessions_median += session_sum/session_total;
							
							session_sum = 0;
							session_total = 1;
							newsession = true;
						} else {
//							session_time += dif;
							last_sec = sec;
							session_sum += dif;
							session_total++;
							newsession = false;
						}
					}
				}
				if(!newsession) {
//					session_time += session_sum;
//					sessions_total += session_total;
					session_time += session_sum;
					try {
						sessions_median += session_sum/session_total;
					} catch (ArithmeticException e) {
						sessions_median = 0.0;
					}
				}
				String session_line = session_time+";"+(sessions_median/sessions_qtd)+";"+sessions_qtd;
				String codeString = codes[0]+";"+codes[1]+";"+codes[2]+";"+codes[3]+";"+codes[4];
				context.write(key, new Text(uReq+";"+session_line+";"+codeString+";"+ttl_bytes+";"+get+";"+put+";"+post+";"+del+";"));
				String line = "";
				for(int i=0;i<l1.size()-1;i++) {
					line += l1.get(i)+":QTD>"+l2.get(i)+";NEXT:";
				}
				mos.write("URL", key, new Text(line));
				/* Parâmetros: ao todo: 14 coluna iniciais
				Requisições por usuário:	uReq
				Tempo total:				session_sum
				Tempo médio por sessão:		sessions_median
				Total de sessões:			sessions_qtd
				Códigos de info:			Code[0]
    			Códigos de sucesso:			Code[1]
    			Códigos de Red:				Code[2]
    			Códigos de Erros cli:		Code[3]
				Códigos de Erros ser:		Code[4]
				Total de bytes Trans.:		ttl_bytes
				Total de Métodos GET:		get
				Total de Métodos PUT:		put
				Total de Métodos POST:		post
				Total de Métodos DEL:		del
				Parâmetros que virão primeiro, antes das colunas de URLs*/
			} /*else if(key.toString().matches("URL:"+Resources.REGEX_IP)) {
//				List<URL> l = new ArrayList<>();
				List<String> l1 = new ArrayList<>();
				List<Integer> l2 = new ArrayList<>();
				for(Text t : values) {
					String url = t.toString();
					if(l1.contains(url)) {
						int i = l1.indexOf(url);
						l2.set(i,l2.get(i)+1);
					} else {
						l1.add(url);
						l2.add(1);
					}
//					URL u = new URL(st.nextToken(),1);
//					if(l.contains(u)) {
//						int i = l.indexOf(u);
//						l.set(i, ne URL(u.url,u.qtd+1));
//					} else {
//						l.add(u);
//					}
				}
				String line = "";
				for(int i=0;i<l1.size()-1;i++) {
					line += l1.get(i)+":QTD>"+l2.get(i)+";NEXT:";
				}
//				for(URL u : l) {line.concat(u.toString()+";");
//				}
				mos.write("URL", new Text(new StringTokenizer(key.toString(),"URL:").nextToken()), new Text(line));
			}*/ else /*if(key.toString().matches(Resources.REGEX_URL))*/ {
				mos.write("KnownURL", key, null);
			}
		}
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			mos.write("Meta", new Text("IPs"), new Text(String.valueOf(ip_list.size())));
//			mos.write("Meta", new Text("Visits"), new Text(String.valueOf(session_list.size())));
//		}
	}
	
	public static class UrlMapperParam extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("	");
			context.write(new Text(v[0]), new Text("1>"+v[1]));
		}
	}
	
	public static class UrlMapperParse extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("	");
			String urls = "2>";
			if(v.length>1) {
				urls += v[1];
			}
			context.write(new Text(v[0]), new Text(urls));			
		}
	}
	
	public static class UrlReducer extends Reducer<Text, Text, Text, Text>{
		/*private List<String> KnownURL_list = new ArrayList<>();
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
//			Path url_path = new Path(conf.get(Setup.JOB_PATH)+"/data.url");
//			RemoteIterator<FileStatus> lfs_knownurl = fs.listStatusIterator(url_path);
//			while(lfs_knownurl.hasNext()) {
//				Path path = lfs_knownurl.next().getPath();
			URI[] uris = context.getCacheFiles();
			for(URI uri : uris) {
				Path path = new Path(uri.getPath());
				if(path.getName().matches("(KnownURL-r-)(\\d+)")) {
					FSDataInputStream in = fs.open(path);
					BufferedReader br = new BufferedReader(new InputStreamReader(in));
					String known_url;
			        while ((known_url = br.readLine()) != null){
			        	KnownURL_list.add(known_url);
			        }
					br.close();
					in.close();
				}
			}
		}*/
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			String param_part = null, url_part = "";
			for(Text t : values) {
				String value = t.toString();
				Matcher mat = Pattern.compile("(1>)(.*)|(2>)(.*)").matcher(value);
				if(mat.find()) {
					if(mat.group(1)!=null) {
//						System.out.println("Found 1");
						param_part = mat.group(2);
					} else if(mat.group(3)!=null) {
//						System.out.println("Found 2");
						String v = mat.group(4);
						List<String> url_list = new ArrayList<>();
						List<Integer> qtd_list = new ArrayList<>();
//						String line = "";
						if(!v.isEmpty()) {
							String[] val = v.split(";NEXT:");
							for(String st : val) {
//								System.out.println(st);
								String[] lurl = st.split(":QTD>");
								lurl[0] = Resources.urlPruner(lurl[0]);
								if(url_list.contains(lurl[0])) {
									int i = url_list.indexOf(lurl[0]);
									qtd_list.set(i,qtd_list.get(i)+Integer.parseInt(lurl[1]));
								} else {
									url_list.add(lurl[0]);
									qtd_list.add(Integer.parseInt(lurl[1]));
								}
//								url_list.add(lurl[0]);
//								qtd_list.add(Integer.parseInt(lurl[1]));
							}
						}
						URI[] uris = context.getCacheFiles();
						for(URI uri : uris) {
							Path path = new Path(uri.getPath());
							if(path.getName().matches("(KnownURL-r-)(\\d+)")) {
								FSDataInputStream in = fs.open(path);
								BufferedReader br = new BufferedReader(new InputStreamReader(in));
								String known_url;
						        while ((known_url = br.readLine()) != null){
//						        	KnownURL_list.add(known_url);
						        	if(url_list.contains(known_url)) {
										int i = url_list.indexOf(known_url);
										url_part += qtd_list.get(i)+";";
									} else url_part += "0;";
						        }
								br.close();
								in.close();
							}
						}
				        /*for(String known_url : KnownURL_list) {
				        	if(url_list.contains(known_url)) {
								int i = url_list.indexOf(known_url);
								url_part += qtd_list.get(i)+";";
							} else url_part += "0;";
				        }*/
					}
				}
			}
			context.write(key, new Text(param_part+url_part));
		}
	}
	
	public static class NormMapper extends Mapper<Object, Text, Text, Text>{
//		List<Integer> param_size = new ArrayList<>();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString(),"\t ;");
			@SuppressWarnings("unused")
			String ip = st.nextToken();
			int i = 0;
			while(st.hasMoreTokens()) {
				String index = String.valueOf(++i);
				context.write(new Text("Param_"+"0".repeat(7-index.length())+index), new Text(st.nextToken()));
				context.write(new Text("param-var"), new Text("1"));
//				param_size.add(1);
			}
			context.write(new Text("param-var"), new Text("2"));
			/*Configuration conf = context.getConfiguration();
			try {
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "param", i);
			} catch (FileNotFoundException | RemoteException e) {}*/
//			context.write(new Text(Setup.MAX_REQ), 	new Text(st.nextToken()));
//			context.write(new Text(Setup.MAX_SEC),	new Text(st.nextToken()));
//			context.write(new Text(Setup.MAX_GET),	new Text(st.nextToken()));
//			context.write(new Text(Setup.MAX_PUT),	new Text(st.nextToken()));
//			context.write(new Text(Setup.MAX_POST),	new Text(st.nextToken()));
//			context.write(new Text(Setup.MAX_DEL),	new Text(st.nextToken()));
		}
		/*protected void cleanup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "param", param_size.size());
		}*/
	}
	
	public static class NormReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			if(key.toString().equals("param-var")) {
				Integer total_param = 0;
				Integer total_lines = 0;
				for(Text t : values) {
					if(t.toString().equals("1")) total_param++;
					else if(t.toString().equals("2")) total_lines++;
				}
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "param",total_param/total_lines);
			} else {
			Double max_val = 0.0;
			Double sum_val = 0.0;
			int total_val = 0;
			boolean f = true;
			/*
			 * Média
			 * 		μ = soma/total
			 * Desvio Padrão
			 * 		σ = √(Σ Xi-μ²)
			 * normalização
			 * 		(Xi-μ)/σ
			 */
			List<Double> list = new ArrayList<>();
			for(Text t : values) {
				Double val = Double.parseDouble(t.toString());
				list.add(val);
				sum_val += val;
				/*if(val != 0)*/ total_val++;
				if(f) { 
					max_val = val;
					f = false;
				} else if(val>max_val) max_val = val;
			}
			Double variance = 0.0;
			for(Double value : list) {
				try {
					variance += Math.pow(value-(sum_val/total_val),2);
				} catch(NotANumberException | ArithmeticException e) {
					variance = 0.0;
				}
			}
			variance = Math.sqrt(variance);
			List<Integer> listBin = new ArrayList<>();
			Integer bin_div = conf.getInt(Setup.HIST_BIN_DIV,10);
			int i = 0;
			while(i++<bin_div) {
				listBin.add(0);
			}
//			String line = "";
			for(Double t : list) {
				Double v;
				if(variance==0.0) v = 0.0;
				else {
					try {
						v = Math.abs(t-(sum_val/total_val))/variance;
					} catch(ArithmeticException e) {
						v = 0.0;
					}
					if(Double.isNaN(v)) v = 0.0;
				}
//				line = v+";";
				Double bin_size = 1.0/bin_div;
				Double cur_bin = bin_size;
				i=0;
				while(i<bin_div) {
					if(v >= cur_bin-bin_size && v < cur_bin) {
						Integer number = listBin.get(i);
						listBin.set(i,number+1);
						break;
					}
					cur_bin += bin_size;
					i++;
				}
			}
			mos.write("VarianceMeta", key, new Text(sum_val+";"+total_val+";"+variance));
			i=0;
			for(Integer bin_val : listBin) {
				String index = String.valueOf(++i);
				index = "0".repeat(String.valueOf(bin_div).length()-index.length())+index;
//				context.write(new Text(key.toString()+"_"+(++i)), new Text(String.valueOf(bin_val)));
				mos.write("Histogram", new Text(key.toString()+"-bin_"+index), new Text(String.valueOf(bin_val)));
			}
			/*Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), key.toString(), max_val);
			Matcher m = Pattern.compile(".*(max_)(\\w+).*").matcher(key.toString());
			if(m.find()) {
//				context.write(new Text("sum_"+m.group(2)), new Text(String.valueOf(sum_val)));
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "sum_"+m.group(2), sum_val);
				Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), "total_"+m.group(2), total_val);
			}*/
			}
		}
	}
}
