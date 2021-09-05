package preproc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import model.WebLog;
import resources.Resources;
import resources.Setup;

public class FileParser {
	public static class FileRegexMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Matcher mat = Pattern.compile(".*(bot).*", Pattern.CASE_INSENSITIVE).matcher(value.toString());
			if (!mat.find()) {
				Configuration c = context.getConfiguration();
				WebLog wl = new WebLog(value.toString(), c.get(Setup.WORD_WANTED), c.get(Setup.WORD_UNWANTED));
				if (!wl.getIp().isEmpty() && wl.getUrl() != null) {
					context.write(new Text(wl.getIp()), new Text(wl.getSeconds() + ">next>" + wl.getCode() + ">next>"
							+ wl.getBytes() + ">next>" + wl.getMethod() + ">next>" + wl.getUrl()));
				}
			}
		}
	}

	public static class FileRegexReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		List<Integer> total_n;
		List<String> known_url_list;

		protected void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
			total_n = new ArrayList<>();
			known_url_list = new ArrayList<>();
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
				uReq++;
				total_n.add(1);
				seconds = Long.parseLong(value[0]);
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
				v = Integer.parseInt(value[1]);
				int code = 100;
				for (int i = 0; i < codes.length; i++) {
					if (v >= code && v <= (code + 99)) {
						codes[i] += 1;
					}
					code += 100;
				}
				ttl_bytes += Integer.parseInt(value[2]);
				m = value[3];
				if (m.matches(".*(GET).*")) {
					get++;
				} else if (m.matches(".*(PUT).*")) {
					put++;
				} else if (m.matches(".*(POST).*")) {
					post++;
				} else if (m.matches(".*(DELETE).*")) {
					del++;
				}
				String url = value[4];
				if (l1.contains(url)) {
					int i = l1.indexOf(url);
					l2.set(i, l2.get(i) + 1);
				} else {
					l1.add(url);
					l2.add(1);
				}
				String pruned_url = Resources.urlPruner(url);
				if (!known_url_list.contains(pruned_url)) {
					known_url_list.add(pruned_url);
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
			context.write(key, new Text(uReq + ";" + session_line + ";" + codeString + ";" + ttl_bytes + ";" + get + ";"
					+ put + ";" + post + ";" + del + ";"));
			String line = "";
			for (int i = 0; i < l1.size() - 1; i++) {
				line += l1.get(i) + ":QTD>" + l2.get(i) + ";NEXT:";
			}
			mos.write("URL", key, new Text(line));
			/*
			 * Parâmetros: ao todo: 14 coluna iniciais Requisições por usuário: uReq Tempo
			 * total: session_sum Tempo médio por sessão: sessions_median Total de sessões:
			 * sessions_qtd Códigos de info: Code[0] Códigos de sucesso: Code[1] Códigos de
			 * Red: Code[2] Códigos de Erros cli: Code[3] Códigos de Erros ser: Code[4]
			 * Total de bytes Trans.: ttl_bytes Total de Métodos GET: get Total de Métodos
			 * PUT: put Total de Métodos POST: post Total de Métodos DEL: del Parâmetros que
			 * virão primeiro, antes das colunas de URLs
			 */
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Resources.writeFileVar(conf, conf.get(Setup.JOB_PATH), Setup.N_TOTAL, total_n.size());
			for (String url : known_url_list) {
				mos.write("KnownURL", new Text(url), null);
			}
		}
	}
}
