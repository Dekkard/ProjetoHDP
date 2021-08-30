package resources;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class Resources {
	public static String REGEX_IP = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
	public static String REGEX_DATE = "(\\d{1,2}\\/(\\w{3}|\\d{1,2})\\/\\d{4})";
	public static String REGEX_TIME = "(\\d{2}:\\d{2}:\\d{2})" ;
	public static String REGEX_CODE = "(\\d{3})";
	public static String REGEX_BYTES = "(\\d+)";
	public static String REGEX_GMT = "([-+]{1}\\d{4})";
	//private static String REGEX_URL = "((\\/\\w+([.|%]\\w+)?|\\/[.|%]*)*)";
	public static String REGEX_URL_REF = "((http[s]?:\\/\\/|ftp:\\/\\/)?(w{3}\\.)?([A-Za-z]+(\\.[A-Za-z]+)+|\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(\\:\\d+)?)+)((\\/[\\w%\\-=?:&/*§_)($#@!+;\\.]+)+)"; //4: Dominio // 7: End. Subsequentes
	public static String REGEX_URL_PRUNE = "\\/(?=.*?[A-Za-z])(?=.*?[0-9])(?=.*?[#?!@$%^&*-]).*|(?=.*?[A-Z])(?=.*?[0-9]).*|.+\\.\\w+";
	public static String REGEX_URL = "(\\/[^\\/]+)+";
	public static String REGEX_URL2 = "((\\/([A-Za-z0-9\\.\\-+)(*&¨%$#@?=:;\\/_]+)+)+)";
	public static String REGEX_PROTOCOL = "(HTTP\\/\\d+(\\.\\d)?)";
	public static String REGEX_METHOD = "(GET|POST|PUT|DELETE|HEAD|TRACE)";
	
	public static Double decScale(Double number, int scale) {
		return new BigDecimal(number).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
	}
	public static Double decScale(String number, int scale) {
		return new BigDecimal(number).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
	}
	private static <T> T addG(T t) {
		return t;
	}
	public static Integer compare(Integer one, Integer two, boolean isMax) {
		if(one > two) return isMax?one:two;
		else return isMax?two:one;
	}
	public static Double compare(Double one, Double two, boolean isMax) {
		if(one > two) return isMax?one:two;
		else return isMax?two:one;
	}
	@SuppressWarnings("unchecked")
	public static <T> List<List<T>> readList(Configuration conf, Class<?> c,  String filename, String Path) throws IllegalArgumentException, IOException {
		List<List<T>> lists = new ArrayList<>();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(Path+"/"+filename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = "";
        while ((line = br.readLine()) != null){
        	List<T> list = new ArrayList<>();
            StringTokenizer st = new StringTokenizer(line,";");
            while(st.hasMoreTokens()) {
            	if(c == String.class) list.add((T) addG(st.nextToken()));
            	else if(c == Double.class) list.add((T) addG(Double.parseDouble(st.nextToken())));
            	else if(c == Integer.class) list.add((T) addG(Integer.parseInt(st.nextToken())));
            }
            lists.add(list);
        }
		br.close();
		in.close();
		return lists;
	}
	public static List<String> readStringList(Configuration conf,  String filename, String Path) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(Path+"/"+filename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
    	List<String> list = new ArrayList<>();
        while ((line = br.readLine()) != null){
        	list.add(line);
        }
		br.close();
		in.close();
		return list;
	}
	public static List<String> readStringList(Configuration conf, Path path) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
    	List<String> list = new ArrayList<>();
        while ((line = br.readLine()) != null){
        	list.add(line);
        }
		br.close();
		in.close();
		return list;
	}
	public static <T> void writeList(Configuration conf, List<List<T>> lists, String filename, String path) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path(path+"/"+filename));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		for(List<T> nc : lists) {
			String line = "";
			for(T d: nc) {
				line += d+";";
			}
			bw.write(line+"\n");
		}
		bw.close();
		out.close();
	}
	public static <T> void appendFile(Configuration conf, T line, String filename, String path) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(path+"/"+filename);
		FSDataOutputStream out = fs.exists(p)?fs.append(p):fs.create(p);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		bw.write(line+"\n");
		bw.close();
		out.close();
	}
	public static boolean compareLists(Configuration conf, String listname1, String listname2, String path1, String path2) throws IllegalArgumentException, IOException {
		List<List<Double>> list1 = readList(conf,Double.class,listname1,path1);
		List<List<Double>> list2 = readList(conf,Double.class,listname2,path2);
		Double cp = Double.parseDouble(conf.get(Setup.ERROR_MARGIN));
		for(int i=0;i<list1.size();i++) {
			for(int j=0;j<list1.get(i).size();j++) {
				if(Math.abs(list1.get(i).get(j)-list2.get(i).get(j)) > cp) {
					return true;
				}
			}
		}
		return false;
	}
	public static List<Double> listInitZero(int d){
		List<Double> list = new ArrayList<>();
		while(list.size() < d) {
			list.add(0.0);
		}
		return list;
	}
	public static <T> void printList(List<List<T>> list, String delim, boolean showIndex) {
		int i = 1;
		for(List<T> ls : list) {
			if(showIndex) System.out.print((i++)+": ");
			for(T l : ls) {
				System.out.print(l+delim);
			}
			System.out.println("");
		}
	}
	public static <T> void writeFileVar(Configuration conf,String path, String filename, T value) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path(path+"/var/"+filename));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		bw.write(value+"\n");
		bw.close();
		out.close();
	}
	@SuppressWarnings("unchecked")
	public static <T> T readFileVar(Configuration conf, Class<?> c, String path, String filename) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(path+"/var/"+filename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		T var = null;
		if(c == Integer.class) var = (T) addG(Integer.parseInt(br.readLine()));
		else if(c == Double.class) var = (T) addG(Double.parseDouble(br.readLine()));
		else if(c == String.class) var = (T) addG(br.readLine());
		br.close();
		in.close();
		return var;
	}
	public static void initCentroid(Configuration conf, String filename) throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(conf.get(Setup.CENTROID_CUR_PATH)+"/"+filename);
		FSDataOutputStream out = fs.create(p);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		int k = conf.getInt(Setup.K_CLUSTER_SIZE,5);
		int d = conf.getInt(Setup.D_PARAM_SIZE,15);
		for(int i = 0; i < k; i++) {
			String centroid = "";
			for(int j = 0; j < d; j++) {
				centroid += Resources.decScale((1.0/(k+1.0))*(i+1.0),Integer.parseInt(conf.get(Setup.USCALE)))+";";
			}
			bw.write(centroid+"\n");
		}
		bw.close();
		out.close();
	}
	public static String urlPruner(String line) {
		Matcher mat = Pattern.compile("(\\/[^\\/]+)").matcher(line);
		String url_f = "";
		while(mat.find()) {
			String url = mat.group(1);
			if(!url.matches("((?=.*?[A-Za-z])(?=.*?[0-9])|(?=.*?(\\.[a-z]+))|(?=.*?[?\\-_=+*&%$#@!])).*"))
				if(url.matches("(\\/\\d+)")) url_f+="/id_num";
				else url_f+=url;
		}
		return url_f;
	}
	public static String timeParse(Long s) {
		Integer hour = null,min=null,sec=null;
		if(s>=3600) hour = (int) (s/3600);
		if(s>=60) min = (int) ((s%3600)/60);
		sec = (int) ((s%3600)%60);
		return ((s>=3600)?(hour+"h"):"")+((s>=60)?(min+"m"):"")+sec+"s";
	}
	public static String createFilter(String words) {
		String[] word = words.split(",");
		String regex_filter = "";
		boolean f = true;
		if(!word[0].isEmpty()) {
			regex_filter += ".*(";
			f = true;
			for(String w : word) {
				regex_filter += (f?"":"|")+w;
				f = false;
			}
			regex_filter += ").*";
		}
		return regex_filter;
	}
	public static String createUnfilter(String words) {
		String[] word = words.split(",");
		String regex_url_unfilter = "";
		boolean f = true;
		if(!word[0].isEmpty()) {
			regex_url_unfilter = ".*(";
			for(String u : word) {
				regex_url_unfilter += (f?"":"|")+u;
				f = false;
			}
			regex_url_unfilter += ").*";
		}
		return regex_url_unfilter;
	}
	public interface StoredMethod {
		void func(String line);
	}
	public interface StoredMethod2 {
		Integer funcInt(String line);
	}
	public static void readFiles(FileSystem fs, Path path, String fname_regex, StoredMethod sm) throws FileNotFoundException, IOException {
		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(path);
		while(lfs.hasNext()) {
			Path p = lfs.next().getPath();
			if(p.getName().matches(fname_regex)) {
				FSDataInputStream in = fs.open(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String rl;
		        while ((rl = br.readLine()) != null) {
		        	sm.func(rl);
		        }
				br.close();
				in.close();
			}
		}
	}
	public static Integer readFiles1(FileSystem fs, Path path, String fname_regex, StoredMethod2 sm) throws FileNotFoundException, IOException {
		int i = 0;
		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(path);
		while(lfs.hasNext()) {
			Path p = lfs.next().getPath();
			if(p.getName().matches(fname_regex)) {
				FSDataInputStream in = fs.open(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String rl;
		        while ((rl = br.readLine()) != null) {
		        	i += sm.funcInt(rl);
		        }
				br.close();
				in.close();
			}
		}
		return i;
	}
	public static List<Double> normalize6(Configuration conf, String path, String value) throws IOException {
//		Double tr = Resources.readFileVar(Double.class, path, filename, FileSystem.get(conf));
		StringTokenizer st = new StringTokenizer(value,";");
		List<Double> listVar = new ArrayList<>();
		
		listVar.add(Double.parseDouble(st.nextToken())/(Double) Resources.readFileVar(conf, Double.class, path, Setup.MAX_REQ));
		listVar.add(Double.parseDouble(st.nextToken())/(Double) Resources.readFileVar(conf, Double.class, path, Setup.MAX_SEC));
		listVar.add(Double.parseDouble(st.nextToken())/(Double) Resources.readFileVar(conf, Double.class, path, Setup.MAX_GET));
		listVar.add(Double.parseDouble(st.nextToken())/(Double) Resources.readFileVar(conf, Double.class, path, Setup.MAX_PUT));
		listVar.add(Double.parseDouble(st.nextToken())/(Double) Resources.readFileVar(conf, Double.class, path, Setup.MAX_POST));
		listVar.add(Double.parseDouble(st.nextToken())/(Double) Resources.readFileVar(conf, Double.class, path, Setup.MAX_DEL));
		
		return listVar;
	}
	public static List<Double> normalize(Configuration conf, String path, String value) throws IOException {
		List<Double> normList = new ArrayList<>();
		RemoteIterator<FileStatus> lfs = FileSystem.get(conf).listStatusIterator(new Path(path));
		while(lfs.hasNext()) {
			Path p = lfs.next().getPath();
			if(p.getName().matches("(part-r-)(\\d+)")) {
				FileSystem fs = FileSystem.get(conf);
				FSDataInputStream in = fs.open(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String val;
//		    	List<String> list = new ArrayList<>();
				String[] values = value.split(";");
				int i = 0;
		        while ((val = br.readLine()) != null){
//		        	list.add(known_url);
		        	String[] v = val.split("(Param_\\d+(\\s)+)|;");
		        	normList.add( Math.abs(Double.valueOf(values[i++])-(Double.valueOf(v[1])/Double.valueOf(v[2])))/Double.valueOf(v[3]) );
		        }
				br.close();
				in.close();
//				List<String> kurl = Resources.readStringList(conf,path);
//				KnownURL.addAll(kurl);
			}
		}
		return normList;
	}
	public static List<Double> loadVar6(Configuration conf) throws IOException{
		List<Double> list = new ArrayList<>();
		list.add(Resources.readFileVar(conf, Double.class, conf.get(Setup.JOB_PATH), Setup.MAX_REQ));
		list.add(Resources.readFileVar(conf, Double.class, conf.get(Setup.JOB_PATH), Setup.MAX_SEC));
		list.add(Resources.readFileVar(conf, Double.class, conf.get(Setup.JOB_PATH), Setup.MAX_GET));
		list.add(Resources.readFileVar(conf, Double.class, conf.get(Setup.JOB_PATH), Setup.MAX_PUT));
		list.add(Resources.readFileVar(conf, Double.class, conf.get(Setup.JOB_PATH), Setup.MAX_POST));
		list.add(Resources.readFileVar(conf, Double.class, conf.get(Setup.JOB_PATH), Setup.MAX_DEL));
		return list;
	}
	public static List<String> htmlEncodeList() {
		List<String> array = new ArrayList<>();
		array.add(" ");array.add("!");array.add("\"");array.add("#");array.add("$");array.add("%");array.add("&");array.add("'");array.add("(");array.add(")");array.add("*");array.add("+");array.add(",");array.add("-");array.add(".");array.add("/");array.add("0");array.add("1");array.add("2");array.add("3");array.add("4");array.add("5");array.add("6");array.add("7");array.add("8");array.add("9");array.add(":");array.add(";");array.add("<");array.add("=");array.add(">");array.add("?");array.add("@");array.add("A");array.add("B");array.add("C");array.add("D");array.add("E");array.add("F");array.add("G");array.add("H");array.add("I");array.add("J");array.add("K");array.add("L");array.add("M");array.add("N");array.add("O");array.add("P");array.add("Q");array.add("R");array.add("S");array.add("T");array.add("U");array.add("V");array.add("W");array.add("X");array.add("Y");array.add("Z");array.add("[");array.add("\\");array.add("]");array.add("^");array.add("_");array.add("`");array.add("a");array.add("b");array.add("c");array.add("d");array.add("e");array.add("f");array.add("g");array.add("h");array.add("i");array.add("j");array.add("k");array.add("l");array.add("m");array.add("n");array.add("o");array.add("p");array.add("q");array.add("r");array.add("s");array.add("t");array.add("u");array.add("v");array.add("w");array.add("x");array.add("y");array.add("z");array.add("{");array.add("|");array.add("}");array.add("~");array.add(" ");array.add("`");array.add("HOP");array.add("‚");array.add("ƒ");array.add("„");array.add("…");array.add("†");array.add("‡");array.add("ˆ");array.add("‰");array.add("Š");array.add("‹");array.add("Œ");array.add("RI");array.add("Ž");array.add("SS3");array.add("DCS");array.add("‘");array.add("’");array.add("“");array.add("”");array.add("•");array.add("–");array.add("—");array.add("˜");array.add("™");array.add("š");array.add("›");array.add("œ");array.add("OSC");array.add("ž");array.add("Ÿ");array.add(" ");array.add("¡");array.add("¢");array.add("£");array.add("¤");array.add("¥");array.add("¦");array.add("§");array.add("¨");array.add("©");array.add("ª");array.add("«");array.add("¬");array.add("­");array.add("®");array.add("¯");array.add("°");array.add("±");array.add("²");array.add("³");array.add("´");array.add("µ");array.add("¶");array.add("·");array.add("¸");array.add("¹");array.add("º");array.add("»");array.add("¼");array.add("½");array.add("¾");array.add("¿");array.add("À");array.add("Á");array.add("Â");array.add("Ã");array.add("Ä");array.add("Å");array.add("Æ");array.add("Ç");array.add("È");array.add("É");array.add("Ê");array.add("Ë");array.add("Ì");array.add("Í");array.add("Î");array.add("Ï");array.add("Ð");array.add("Ñ");array.add("Ò");array.add("Ó");array.add("Ô");array.add("Õ");array.add("Ö");array.add("×");array.add("Ø");array.add("Ù");array.add("Ú");array.add("Û");array.add("Ü");array.add("Ý");array.add("Þ");array.add("ß");array.add("à");array.add("á");array.add("â");array.add("ã");array.add("ä");array.add("å");array.add("æ");array.add("ç");array.add("è");array.add("é");array.add("ê");array.add("ë");array.add("ì");array.add("í");array.add("î");array.add("ï");array.add("ð");array.add("ñ");array.add("ò");array.add("ó");array.add("ô");array.add("õ");array.add("ö");array.add("÷");array.add("ø");array.add("ù");array.add("ú");array.add("û");array.add("ü");array.add("ý");array.add("þ");array.add("ÿ");
		return array;
	}
	public static List<String> htmlEncodeListIr() {
		List<String> array = new ArrayList<>();
		array.add("۰");array.add("۱");array.add("۲");array.add("۳");array.add("۴");array.add("۵");array.add("۶");array.add("۷");array.add("۸");array.add("۹");array.add("،");array.add("ـ");array.add("؟");array.add("ﺁ");array.add("ﺋ");array.add("ﺀ");array.add("ﺍ");array.add("ﺎ");array.add("ﺏ");array.add("ﺑ");array.add("ﭖ");array.add("ﭘ");array.add("ﺕ");array.add("ﺗ");array.add("ﺙ");array.add("ﺛ");array.add("ﺝ");array.add("ﺟ");array.add("ﭺ");array.add("ﭼ");array.add("ﺡ");array.add("ﺣ");array.add("ﺥ");array.add("ﺧ");array.add("ﺩ");array.add("ﺫ");array.add("ﺭ");array.add("ﺯ");array.add("ﮊ");array.add("ﺱ");array.add("ﺳ");array.add("ﺵ");array.add("ﺷ");array.add("ﺹ");array.add("ﺻ");array.add("ﺽ");array.add("ﺿ");array.add("ﻁ");array.add("░");array.add("▒");array.add("▓");array.add("│");array.add("┤");array.add("╡");array.add("╢");array.add("╖");array.add("╕");array.add("╣");array.add("║");array.add("╗");array.add("╝");array.add("╜");array.add("╛");array.add("┐");array.add("└");array.add("┴");array.add("┬");array.add("├");array.add("─");array.add("┼");array.add("╞");array.add("╟");array.add("╚");array.add("╔");array.add("╩");array.add("╦");array.add("╠");array.add("═");array.add("╬");array.add("╧");array.add("╨");array.add("╤");array.add("╥");array.add("╙");array.add("╘");array.add("╒");array.add("╓");array.add("╫");array.add("╪");array.add("┘");array.add("┌");array.add("█");array.add("▄");array.add("▌");array.add("▐");array.add("▀");array.add("ﻅ");array.add("ﻉ");array.add("ﻊ");array.add("ﻌ");array.add("ﻋ");array.add("ﻍ");array.add("ﻎ");array.add("ﻐ");array.add("ﻏ");array.add("ﻑ");array.add("ﻓ");array.add("ﻕ");array.add("ﻗ");array.add("ﮎ");array.add("ﮐ");array.add("ﮒ");array.add("ﮔ");array.add("ﻝ");array.add("ﻻ");array.add("ﻟ");array.add("ﻡ");array.add("ﻣ");array.add("ﻥ");array.add("ﻧ");array.add("ﻭ");array.add("ﻩ");array.add("ﻬ");array.add("ﻫ");array.add("ﯽ");array.add("ﯼ");array.add("ﯾ");
		return array;
	}
}
