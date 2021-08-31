package tester;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import model.WebLog;
import resources.RegexMatch;
import resources.Resources;
import resources.Setup;

@SuppressWarnings("all")
public class Tester {
	protected static String TOTAL_REQUESTS = "TOTAL_REQUESTS";
	private static String DELIMITER = ", \"[]";
	
	private static Integer timeToSec(int hour, int min, int sec) {
		return hour*3600+min*60+sec;
	}
	
	public static void wordRecon1(String[] args) {
//		StringTokenizer st = new StringTokenizer("10.128.2.1,[20/Feb/2018:14:38:25,GET /fonts/fontawesome-webfont.woff?v=4.6.3 HTTP/1.1,200", DELIMITER);
		StringTokenizer st = new StringTokenizer("54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] \"GET /filter/27|13%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,27|%DA%A9%D9%85%D8%AA%D8%B1%20%D8%A7%D8%B2%205%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,p53 HTTP/1.1\" 200 30577 \"-\" \"Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)\" \"-\"", DELIMITER);
		String url = "";
		int seconds;
		while(st.hasMoreElements()) {
			String t = st.nextToken();
			String rg = new RegexMatch().WordMatch(t);
			if(rg == "URL") {
				url += " "+t;
			} else if(rg == "Timestamp" || rg == "Time"){
				Matcher m = Pattern.compile(".*(\\d+):(\\d+):(\\d+).*").matcher(t);
				if(m.find()) {
					seconds = timeToSec(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)));
					System.out.println(t+": "+rg+" - "+seconds+"s");
				}
			} else {
				System.out.println(t+": "+rg);
			}
		}
		System.out.println(url);
	}
	
	public static void wordRecon2(String[] args) {
		String del = "\"";
		StringTokenizer st = new StringTokenizer("\"192.168.4.25 - - [22/Dec/2016:16:30:52 +0300] \"POST /administrator/index.php HTTP/1.1\" 303 382 \"http://192.168.4.161/DVWA\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.21 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.21\"\"", del);
		while(st.hasMoreElements()) {
			
			String t = st.nextToken();
			System.out.println(t);
		}
	}
	
	public static void wordRecon3(String[] args) {
		Configuration c = new Configuration();
		c.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
		WebLog wl = new WebLog("\"192.168.4.25 - - [22/Dec/2016:16:30:52 +0300] \"POST /administrator/index.php/./.. HTTP/1.1\" 303 382 \"http://192.168.4.161/DVWA\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.21 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.21\"\"","","");
		System.out.println(wl.toString(true,"\n"));
		System.out.println(wl.getDomain());
//		System.out.println(wl.listSubAddress());
		System.out.println();
		WebLog wl1 = new WebLog("204.18.198.248 22/Jan/2019:03:58:15 +0330 GET /image/11/productType/120x90 HTTP/1.1 200 15613 https://www.zanbil.com.ir/m/product/5820/6404/%D8%A2%D8%A8-%D9%BE%D8%B1%D8%AA%D9%82%D8%A7%D9%84-%DA%AF%DB%8C%D8%B1%DB%8C-%DA%AF%D8%A7%D8%B3%D8%AA%D8%B1%D9%88%D8%A8%DA%A9-%D9%85%D8%AF%D9%84-41138 Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1","","");
		System.out.println(wl1.toString(true,"\n"));
		System.out.println(wl1.getDomain());
//		System.out.println(wl1.listSubAddress());
	}
	
	public static List<List<DoubleWritable>> centroidInit(Integer n, Integer k){
		List<List<DoubleWritable>> cs = new ArrayList<>();
		for(int i = 0; i < k; i++) {
			List<DoubleWritable> c = new ArrayList<DoubleWritable>();
			while(c.size() < n) {
				c.add(new DoubleWritable(new BigDecimal(((1.0/(k+1.0))*(i+1.0))).setScale(3, RoundingMode.HALF_EVEN).doubleValue()));
			}
			cs.add(c);
		}
		return cs;
	}
	
	public static void listMaker() {
		int k = 5, n = 3;
		List<List<DoubleWritable>> centroids = centroidInit(n,k);
		for(List<DoubleWritable> cs : centroids) {
			for(DoubleWritable c : cs) {
				System.out.println(c);
			}
			System.out.println("");
		}
	}
	
	public static void compareTest() {
		int m = 0;
		for(int i=1;i<=5;i++) {
			if(m == 0 || m < i) {
				m = i;
			}
		}
		System.out.println(m);
	}
	
	public static void listPrintTest() {
		List<String> line = new ArrayList<>();
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;POST");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15124;GET");
		line.add("1.234.99.77     1;15125;GET");
		line.add("1.234.99.77     1;15125;GET");
		line.add("1.234.99.77     1;15125;GET");
		line.add("1.234.99.77     1;15125;GET");
		line.add("1.234.99.77     1;15125;POST");
		String ip = "";
		int uReq = 0;
		int session;
		int max = 0;
		int min = 0;
		int get = 0, put = 0, post = 0, del = 0;
		for(String t : line) {
			StringTokenizer st = new StringTokenizer(t,"; ");
			ip = st.nextToken();
			int v;
			String m;
			v = Integer.parseInt(st.nextToken());
			uReq += v;
			v = Integer.parseInt(st.nextToken());
			if(min == 0 || v < min) {
				min = v;
			}
			if(max == 0 || v > max) {
				max = v;
			}
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
		session = max-min;
		System.out.println(ip+"\t"+uReq+";"+session+";"+get+";"+put+";"+post+";"+del);
	}
	
	public static void visCentroid(String[] args) throws IOException {
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.6:9001");
		FileSystem fs = FileSystem.get(conf);
		if(args.length > 1) {
			System.err.println("Compares only one pair of centroid and new centroid, pass only the folder.");
			System.exit(2);
		}
		List<List<Double>> centroids = Resources.readList(conf, Double.class, Setup.CENTROID,args[0]);
		List<List<Double>> new_centroids = Resources.readList(conf, Double.class, Setup.NEW_CENTROID,args[0]);
		for(int i=0;i<centroids.size();i++) {
			if(i==0) System.out.println(Setup.CENTROID+"\t".repeat(centroids.get(0).size()*2+1)+Setup.NEW_CENTROID);
			System.out.print(i+": ");
			for(int j=0;j<centroids.get(i).size();j++) {
				System.out.print(Resources.decScale(centroids.get(i).get(j), 8)+"\t");
			}
			System.out.print("\t\t");
			for(int j=0;j<new_centroids.get(i).size();j++) {
				System.out.print(Resources.decScale(new_centroids.get(i).get(j), 8)+"\t");
			}
			System.out.println();
		}
		System.out.println("\nComparation:");
		for(int i=0;i<centroids.size();i++) {
//			System.out.print(i+": ");
			for(int j=0;j<centroids.get(i).size();j++) {
//				System.out.print(j+" ");
				System.out.print(centroids.get(i).get(j)-new_centroids.get(i).get(j)+"\t");
			}
			System.out.println();
		}
		fs.close();
	}
	public static <T> void ddt(Class<?> t) {
		if(t == Integer.class) {
			System.out.println("Integer");
		} else
		if(t == String.class) {
			System.out.println("String");
		} else
		if(t == Double.class) {
			System.out.println("Double");
		}
	}
	public static Integer compare(Integer one, Integer two, boolean max) {
		if(one > two) return max?one:two;
		else return max?two:one;
	}
	public static Double compare(Double one, Double two, boolean max) {
		if(one > two) return max?one:two;
		else return max?two:one;
	}
	public static void main(String[] args) throws IOException {
//		wordRecon1(args);
//		wordRecon2(args);
//		wordRecon3(args);
//		listMaker();
//		compareTest();
//		listPrintTest();
//		System.out.println(new IntWritable(54));
//		StringTokenizer st = new StringTokenizer("TOTAL_REQUESTS:10","TOTAL_REQUESTS:");
//		System.out.println(st.nextToken());
//		System.out.println(1 - new BigDecimal("3.385608744543729E-5").setScale(6, RoundingMode.HALF_EVEN).doubleValue());
//		System.out.println(Double.parseDouble("3.385608744543729E-5"));
//		System.out.println(1-5E-15);
//		System.out.println(TempFileGen.FileNameGen(12));
//		System.out.println(FilenameGen.timeGen());
        /*Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.1.5:9001");
		FileSystem fs = FileSystem.get(conf);
		Path h_path = new Path("/user/hadoop/jobs_11-5-2021/job_0003");
		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(h_path); //{job_(\\w+)}
		while(lfs.hasNext()) {
			String file = lfs.next().getPath().getName();
			if(file.matches("(kmeans_)(\\d+)")) {
				System.out.println(file);
			}
		}
		fs.close();*/
//		System.out.println(FilenameGen.dateGen(true,true));
//		System.out.println(FilenameGen.dateGen(true,false));
//		System.out.println(FilenameGen.dateGen(false,true));
//		System.out.println(FilenameGen.dateGen(false,false));
//		visCentroid(args);
		/*Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.1.6:9001");
		FileSystem fs = FileSystem.get(conf);
		String h_path = "/user/hadoop/jobs_12-5-2021/job_0002/data.centroid";
		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(new Path(h_path)); //{job_(\\w+)}
		List<List<String>> list = new ArrayList<>();
		while(lfs.hasNext()) {
			String file = lfs.next().getPath().getName();
			if(file.matches("(part-r-)(\\d+)")) {
				Resources.readList(conf, list, String.class, file, h_path);
				Resources.printList(list);
			}
		}
		fs.close();*/
		/*Integer d = 10;
		String y = "10";
		List<List<Double>> lld = new ArrayList<>();
		ddt(d.getClass());
		ddt(y.getClass());
		ddt(Double.class);*/
//		System.out.println(FilenameGen.randGen(18));
		/*try {
			System.out.println(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH).parse("22/Jan/2019:12:01:59 +0330").getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}*/
		/*List array = Resources.htmlEncodeListIr();
		String url = "https://www.zanbil.ir/m/product/5820/6404/%D8%A2%D8%A8-%D9%BE%D8%B1%D8%AA%D9%82%D8%A7%D9%84-%DA%AF%DB%8C%D8%B1%DB%8C-%DA%AF%D8%A7%D8%B3%D8%AA%D8%B1%D9%88%D8%A8%DA%A9-%D9%85%D8%AF%D9%84-41138";
		String r = "(\\%([0-9A-F]+))"; 
		Matcher mat = Pattern.compile(r).matcher(url);
		while(mat.find()) {
			//System.out.println(mat.group(1)+" - "+array.get(Integer.parseInt(mat.group(2),16)-32));
			url = url.replaceAll(mat.group(1), array.get(Integer.parseInt(mat.group(2),16)-128).toString());
			//url.replaceAll(r, array.get(Integer.parseInt(mat.group(2),16)-32).toString());
		}
		System.out.println(url);*/
		/*String line1 = "66.249.66.91 - - [25/Jan/2019:08:16:41 +0330] \"GET /filter?f=b852&o=b HTTP/1.1\" 302 0 \"-\" \"Mozilla/5.0 (compatible; GooglebOt/2.1; +http://www.google.com/bOt.html)\" \"-\"";
		String line2 = "213.217.50.131 - - [26/Jan/2019:13:06:56 +0330] \"GET /image/3/brand HTTP/1.1\" 200 2885 \"https://www.zanbil.ir/browse/home-appliances/%D9%84%D9%88%D8%A7%D8%B2%D9%85-%D8%AE%D8%A7%D9%86%DA%AF%DB%8C\" \"Mozilla/5.0 (Windows NT 6.3; rv:64.0) Gecko/20100101 Firefox/64.0\" \"-\"";
		Matcher mat = Pattern.compile(".*(bot).*",Pattern.CASE_INSENSITIVE).matcher(line1);
		if(!mat.find()) {
			System.out.println("Isn't Bot");
		} else System.out.println("Is Bot");
		mat = Pattern.compile(".*([bB]{1}ot).*",Pattern.CASE_INSENSITIVE).matcher(line2);
		if(!mat.find()) {
			System.out.println("Isn't Bot");
		} else System.out.println("Is Bot");*/
		
//		List<String> l = new ArrayList<>();
//		while(st.hasMoreTokens()) {
//			l.add(st.nextToken());
//		}
//		Iterator<String> v = new ArrayIterator<String>(l);
		/*String line = "1;2;3;4;5;6;7;8;9;10;";
		String[] l = line.split(";");
		StringTokenizer st = new StringTokenizer(line,";");

		Iterable<String> v = new ArrayIterator(l);
		System.out.println(Iterables.size(v));
		System.out.println("Consumer:");
		v.forEach(new Consumer<String>() {
			public void accept(String t) {
				System.out.println(t);
			}
		});
		System.out.println("For:");
		for(String n : v) {
			System.out.println(n);
		}*/
		/*for(String n : v) {
			System.out.println(n);
		}*/
		/*String v1 = "2.188.27.28     1;0;0;0;0;0;0;17251;1;0;0;0;";
		String v2 = "2.188.70.241    1;0;0;0;0;0;0;90970;1;0;0;0;";
		String[] sarray = v1.split("\s+");
		System.out.println(sarray.length);
//		for(String s : sarray) System.out.println(s);
		System.out.println(sarray[0]);
		System.out.println(sarray[1]);*/
		
		/*TODO: Teste de manipulação de arquivo do HDFS*/
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
//		FileSystem fs = FileSystem.get(conf);
//		String v_path = "/user/bigdata/jobs_18-08-2021/job_0006/data.vector";
//		String m_path = "/user/bigdata/jobs_18-08-2021/job_0006/data.meta";
//		RemoteIterator<FileStatus> lfs = fs.listStatusIterator(new Path(v_path)); //{job_(\\w+)}
//		while(lfs.hasNext()) {
//			Path file = lfs.next().getPath();
//			if(file.getName().matches("(part-r-)(\\d+)")) {
//				System.out.println(file);
//				FSDataInputStream in = fs.open(file);
//				BufferedReader br = new BufferedReader(new InputStreamReader(in));
//				String line;
//		        while ((line = br.readLine()) != null){
//		        	String[] vector = line.split("	");
//		        	vector = vector[1].split(";");
//		        	RemoteIterator<FileStatus> lfs1 = fs.listStatusIterator(new Path(m_path)); //{job_(\\w+)}
//		        	while(lfs1.hasNext()) {
//		    			Path file1 = lfs1.next().getPath();
//		    			if(file1.getName().matches("(VarianceMeta-r-)(\\d+)")) {
//		    				System.out.println(file1);
//		    				FSDataInputStream in1 = fs.open(file1);
//		    				BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
//		    				String line1;
//		    				String newline1 = "";
//		    				String newline2 = "";
//		    				int i = 0;
//	    					while ((line1 = br1.readLine()) != null){
//		    		        	String[] meta = line1.split("	");
//		    		        	meta = meta[1].split(";");
//    		        			Double x = Double.parseDouble(vector[i]);
//    		        			Double s = Double.parseDouble(meta[0]);
//    		        			Double t = Double.parseDouble(meta[1]);
//    		        			Double d = Double.parseDouble(meta[2]);
//	    		        		Double val;
//	    		        		if(d==0.0) val = 0.0;
//	    		        		else {
//		    		        		try {
//		    		        			val = Math.abs((x-(s/t))/d);
//		    		        		} catch(ArithmeticException e) {
//		    		        			val = 0.0;
//		    		        		}
//		    		        		if(Double.isNaN(val)) val = 0.0;
//	    		        		}
//	    		        		String val1 = "("+x+"|"+t+"|"+s+"|"+d+")";
//	    		        		newline1 += val+";";
//	    		        		newline2 += val1+";";
//	    		        	}
//	    		        	System.out.println(newline1);
//	    		        	System.out.println(newline2);
//	    		        }
//		    		}
//		    		
//		        }
//				br.close();
//				in.close();
//			}
//			/*if(file.getName().matches("(part-r-)(\\d+)")) {
//				System.out.println(file);
//				FSDataInputStream in = fs.open(file);
//				BufferedReader br = new BufferedReader(new InputStreamReader(in));
//				String line;
//		        while ((line = br.readLine()) != null){
//		        	String[] sarray = line.split("	");
//		        	for(String s : sarray) System.out.println(s);
//		        }
//				br.close();
//				in.close();
//			}*/
//			/*if(file.getName().matches("(URL-r-)(\\d+)")) {
//				FSDataInputStream in = fs.open(file);
//				BufferedReader br = new BufferedReader(new InputStreamReader(in));
//				String line;
//		        while ((line = br.readLine()) != null){
//		        	System.out.println(line);
//		        	String[] sarray = line.split("	");
//		        	String ip = sarray[0];
//		        	if(sarray.length > 1) {
//		        		System.out.println(ip);
//			        	String[] val = sarray[1].split(";NEXT:");
//			        	for(String s : val) { 
//			        		String[] url = s.split(":QTD>");
//			        		for(String u : url) System.out.println(url.length+": "+u);
//			        	}
//		        	}
//		        }
//				br.close();
//				in.close();
//			}*/
//			/*if(file.getName().matches("(part-r-)(\\d+)")) {
//				FSDataInputStream in = fs.open(file);
//				BufferedReader br = new BufferedReader(new InputStreamReader(in));
//				String line1,line2;
//		        while ((line1 = br.readLine()) != null && (line2 = br.readLine()) != null ){
//		        	String param = null, url = null;
//		        	
//		        	String[] sarray1 = line1.split("	");
//		        	String ip = sarray1[0];
//		        	
//		        	String[] sarray2 = line2.split("	");
//		        	String ip2 = sarray2[0];
//		        	
//		        	Matcher mat = Pattern.compile("(1>(.+))|(2>(.*))").matcher(sarray1[1]);
//					if(mat.find()) {
//						if(mat.group(2)!=null) {
//							param = mat.group(2);
//						} else if(mat.group(4)!=null) {
//							url = mat.group(4);
//						}
//					}
//					 mat = Pattern.compile("(1>(.+))|(2>(.*))").matcher(sarray2[1]);
//					if(mat.find()) {
//						if(mat.group(2)!=null) {
//							param = mat.group(2);
//						} else if(mat.group(4)!=null) {
//							url = mat.group(4);
//						}
//					}
//					System.out.println(param+url);
//		        }
//				br.close();
//				in.close();
//			}*/
//		}
//		fs.close();
		/*TODO:Fim teste de manipulação de arquivos do HDFS*/
		
		/*String[] wanted = "product".split(",");
		String[] unwanted = "image".split(",");
		String regex_url_filter = "";
		String regex_url_unfilter = "";
		boolean f = true;
		if(!unwanted[0].isEmpty()) {
			regex_url_unfilter = ".*(";
			for(String u : unwanted) {
				regex_url_unfilter += (f?"":"|")+u;
				f = false;
			}
			regex_url_unfilter += ").*";
		}
		if(!wanted[0].isEmpty()) {
			regex_url_filter += ".*(";
			f = true;
			for(String w : wanted) {
				regex_url_filter += (f?"":"|")+w;
				f = false;
			}
			regex_url_filter += ").*";
		}
		System.out.println(regex_url_unfilter);
		System.out.println(regex_url_filter);*/
		/*Matcher mat = Pattern.compile("(\\/[^\\/]+)").matcher("/m/product/2833");
		while(mat.matches()) System.out.println(mat.group(1));*/
		
		/*String line = "66.249.66.194 - - [24/Jan/2019:02:37:56 +0330] \"GET /m/product/2833 HTTP/1.1\" 404 33637 \"-\" \"Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\" \"-\"";
		WebLog wl = new WebLog(line,".*(product).*",".*(image).*");
		System.out.println(wl.toString(true,"\n"));*/
		
		/*Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
		FileSystem fs = FileSystem.get(conf);
		String h_path = "/user/bigdata/access_sample4.log";
		FSDataInputStream in = fs.open(new Path(h_path));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
        while ((line = br.readLine()) != null){
        	System.out.println("Linha: "+line);
//        	WebLog wl = new WebLog(line,".*(product).*",".*(image|filter|search).*");
        	WebLog wl = new WebLog(line,"","(image)");
        	System.out.println("Proc: "+wl.toString(true,"|"));
        }
		br.close();
		in.close();*/
//		System.out.println(Integer.parseInt("Param_00001".split("Param_")[1]));
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
//		FileSystem fs = FileSystem.get(conf);
//		Integer k = 30;
//		Integer bin_size = 25;
//		
////		List<List<String>> centroids = new ArrayList<>();
//		Path path = new Path("/user/bigdata/jobs_19-08-2021/job_0002/data.meta");
//		RemoteIterator<FileStatus> lfs = FileSystem.get(conf).listStatusIterator(path);
//		while(lfs.hasNext()) {
//			Path p1 = lfs.next().getPath();
//			if(p1.getName().matches("(Histogram-r-)(\\d+)")) {
//				FSDataInputStream in = fs.open(p1);
//				BufferedReader br = new BufferedReader(new InputStreamReader(in));
//				String  rl;
//		        while ((rl = br.readLine()) != null){
//		        	String[] bin = rl.split("	");
//	        		System.out.println(bin[0].split("-bin_(\\d+)")[0]);
//		        }
//			}
//		}
//		RemoteIterator<FileStatus> lfs1 = FileSystem.get(conf).listStatusIterator(path);
//		RemoteIterator<FileStatus> lfs2 = FileSystem.get(conf).listStatusIterator(path);
//		List<List<String>> centroids = new ArrayList<>();
//		for(int i=0;i<k;i++) {
//			List<String> c = new ArrayList<>();
//			centroids.add(c);
//			System.out.println("Iniciando Centroid "+(i+1));
//		}
//		System.out.println("Centroids Inciados");
//		while(lfs1.hasNext()) {
//			Path p1 = lfs1.next().getPath();
//			if(p1.getName().matches("(VarianceMeta-r-)(\\d+)")) {
//				while(lfs2.hasNext()) {
//					FSDataInputStream in1 = fs.open(p1);
//					BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
//					String rl1;
//					while ((rl1 = br1.readLine()) != null){
//						Path p2 = lfs2.next().getPath();
//						if(p2.getName().matches("(Histogram-r-)(\\d+)")) {
//							FSDataInputStream in2 = fs.open(p2);
//				        	BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
//							String rl2;
//				        	String[] param = rl1.split("	");
//				        	String p = param[0];
//				        	Double total = Double.parseDouble(param[1].split(";")[1]);
//				        	Double part = total/k;
//				        	Double qtd = 0.0;
//				        	Double qtd_ac = 0.0;
//				        	Double cur_bin = 1.0/bin_size;
//				        	int cur_div = 0;
//				        	System.out.println("Total:"+total+" parte:"+part+" param:"+p+" bin:"+bin_size+" atual:"+cur_bin);
//					        while((rl2 = br2.readLine()) != null && cur_bin < 1){
//				        		/*System.out.print("Bin: "+cur_bin+" ");
//					        	qtd += Integer.parseInt(rl2.split("	")[1]);
//				        		if(qtd>=part) {
//				        			int div = (int) Math.floor(qtd/part);
//				        			qtd_ac += qtd;
//				        			qtd = qtd%part;
//				        			for(int d=cur_div;d<div;d++) {
//	//				        				context.write(new Text(String.valueOf(d)), new Text(p+":"+(cur_bin+((bin_size*d)*(part*qtd_ac)))+";"));
//				centroids.get(d).add(p+":"+(cur_bin+((bin_size*d)*(part/qtd_ac)))+";");
//				        			}
//				        			cur_div = div;
//				        		}
//				        		cur_bin += 1.0/bin_size;*/
//					        	String[] bin = rl2.split("	");
//				        		if(bin[0].split("-bin_(\\d+)")[0].equals(p)) {
//				        			Integer cur_bin_val = Integer.parseInt(bin[1]);
//									qtd += cur_bin_val;
//									qtd_ac += cur_bin_val;
//					        		if(qtd>=part) {
//					        			int div = (int) Math.floor(qtd/part);
//					        			div += cur_div;
////								        qtd_ac += qtd;
//					        			qtd = qtd%part;
//					        			System.out.println("Bin at.:"+cur_bin+"| Tam. Bin:"+bin_size+"| Div ac.:"+div+"| Div. at.:"+cur_div+"| Total:"+total+"| Parte:"+part+"| Qtd at.:"+qtd+"| Qtd ac.:"+qtd_ac);
//					        			for(int d=cur_div;d<div;d++) {
//					        				
//					        				double dnum = (cur_bin)+((1.0/bin_size*d)*(part/qtd_ac));
//					        				String index = String.valueOf(d+1);
//					        				index = "0".repeat(String.valueOf(k).length()-index.length())+index;
//					        				System.out.print(index+" - "+dnum);
////					        				mos.write("Centroids", new Text(String.valueOf(index)), new Text(p+":"+dnum));
////					        				context.write(new Text(String.valueOf(index)), new Text(p+":"+dnum));
//					        			}
//					        			cur_div = div;
//					        		}
//					        		cur_bin += 1.0/bin_size;
//				        		}
//				        	}
//					        System.out.println();
//				        }
//					}
//		        }
//			}
//		}
//		int j=1;
//		for(List<String> centroid : centroids) {
//			System.out.print((j++)+":");
//			for(String c : centroid) {
//				System.out.print(c+" ");
//			}
//			System.out.println();
//		}
		
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
//		FileSystem fs = FileSystem.get(conf);
//		Integer k = 25;
//		Integer bin_size = 25;
////		List<List<String>> centroids = new ArrayList<>();
//		Path path1 = new Path("/user/bigdata/jobs_19-08-2021/job_0002/data.kmeans/kmeans_14");
//		RemoteIterator<FileStatus> compare_files_survey = fs.listStatusIterator(path1);
//		int rounds = 1;
//		int kluster = 25;
//		List<Integer> compare_centroids = new ArrayList<>();
//		while(compare_files_survey.hasNext()) {
////			System.out.println(compare_files_survey.next().getPath());
//			Path p1 = new Path(compare_files_survey.next().getPath()+"/compare");
//			boolean break_kmeans = false;
//			int compare_list_index = 0;
//			int compare_complete_flag = 0;
//			RemoteIterator<FileStatus> compare_files_centroid = fs.listStatusIterator(p1);
//			while(compare_files_centroid.hasNext()) {
//				Path p = compare_files_centroid.next().getPath();
//				if(p.getName().matches("(Compare-r-)(\\d+)")) {
//					FSDataInputStream in = fs.open(p);
//					BufferedReader br = new BufferedReader(new InputStreamReader(in));
//					String rl;
//					while((rl=br.readLine())!=null) {
//						String[] comp = rl.split("	");
//						String centroid_number = comp[0];
//						int centroid_compare = Integer.parseInt(comp[1]);
//						if(rounds>1) {
//							Integer centroid_compare_old = compare_centroids.get(compare_list_index);
//							System.out.print("Replace "+centroid_number+" -> "+centroid_compare+" to "+ centroid_compare_old);
//							if(centroid_compare_old==centroid_compare) {
//								compare_complete_flag++;
//							}
//							System.out.println(", completion: "+compare_complete_flag);
//							compare_centroids.set(compare_list_index, centroid_compare);
//						} else {
//							System.out.println("Add "+centroid_number+" -> "+centroid_compare);
//							compare_centroids.add(centroid_compare);
//						}
//						compare_list_index++;
//					}
//					System.out.println(compare_complete_flag+" - "+kluster);
//					if(compare_complete_flag == kluster) {
//						System.out.println("stop");
//						break_kmeans = true;
//						break;
//					} else System.out.println("Continue");
//				}
//			}
//			if(break_kmeans) break;
//			rounds++;
//		}
		/*String index = String.valueOf(0);
		index = "0".repeat(String.valueOf(25).length()-index.length())+index;
		System.out.println(String.valueOf(index));*/
		/*List<Double> list = new ArrayList<>();
		System.out.println("Tendando adicionar elemento a lista.");
		try {
			System.out.println("Existe: "+list.get(0));
		} catch (IndexOutOfBoundsException e) {
			System.out.println("Não Existe: Adicionado");
			list.add(1.0);
		}*/
		/*String c1 = "nc>123.132.123.312	0;0;0;0;1;";
		String[] separe1 = c1.split("	|>");
		for(String s : separe1) {
			System.out.println(s);
		}
		String c2 = "c>0;0;0;0;1;";
		String[] separe2 = c2.split("	|>");
		for(String s : separe2) {
			System.out.println(s);
		}*/
//		String regex_date = "(\\d{1,2}\\/(\\w{3}|\\d{1,2})\\/\\d{4})";
//		String regex_time = "(\\d{1,2}:\\d{1,2}:\\d{1,2})";
//		/*List<Long> sessions = new ArrayList<>();
//		String REGEX_IP = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
//		String REGEX_DATE = "(\\d{2}\\/(\\w{3}|\\d{2})\\/\\d{4})";
//		String REGEX_TIME = "(\\d{2}:\\d{2}:\\d{2})";
//		String REGEX_METHOD = "((GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH) ((\\/\\S+)+))";
//		String REGEX_CODE = "((\\d{3})\\s(\\d+))";
//		String REGEX_PROTOCOL = "(HTTP\\/\\d+(\\.\\d)?)";*/
		
		
//		Long timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
//		FileSystem fs = FileSystem.get(conf);
//		Path path = new Path("/user/bigdata");
////		Path path3 = new Path("/user/bigdata/jobs/");
//		String fnamek = "(KnownURL-r-)(\\d+)";
//		String fnameh = "(Histogram-r-)(\\d+)";
//		String fname4 = "access_sample4.log";
//		String fname5 = "access_sample5.log";
//		String fname = "access.log";
//		List<String> url_list = new ArrayList<>();
//		List<Integer> url_qtd = new ArrayList<>();
//		Resources.readFiles(fs, path, fname5 , (line)->{
////			System.out.println(line);
//			WebLog wl = new WebLog(line,".*(product).*",".*(image|filter|search|rss|site|producttype).*");
//			if(wl.getUrl()!=null) {
//				String url = Resources.urlPruner(wl.getUrl());
////				System.out.println(url);
//				if(url_list.contains(url)) {
//					int i = url_list.indexOf(url);
//					url_qtd.set(i, url_qtd.get(i)+1);
//					
//				} else {
//					url_list.add(url);
//					url_qtd.add(1);
//				}
//			}
//		});
//		for(int i=0;i<url_list.size();i++) {
//			System.out.println(url_list.get(i)+" >> "+url_qtd.get(i));
//		}
//		System.out.println("Tempo Ex.: "+(LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3))-timenow));

		
//		/*int i = Resources.readFiles1(fs, path1, fname, (line)->{
//			return 1;
//		});
//		System.out.println(i);*/
//		Integer num_bots = 0, num_lines = 0;
//		InputStream in = new FileInputStream(new File("C:\\Users\\gusta\\Downloads\\Estudos\\Dataset\\access_sample4.log"));
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
//		FileSystem fs = FileSystem.get(conf);
//		Path p = new Path("/user/bigdata/access_sample4.log");
//		FSDataInputStream in = fs.open(p);
//		
//		BufferedReader br = new BufferedReader(new InputStreamReader(in));
//		String rl;
//		Long timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
//		System.out.print("Begin");
//		while ((rl = br.readLine()) != null) {
//			WebLog wl = new WebLog(rl,Resources.createFilter("product"),Resources.createFilter("image,filter,search,rss"));
//        	System.out.println(wl.toString(true,", "));
//			num_lines++;
//			Matcher mat = Pattern.compile(".*(bot).*",Pattern.CASE_INSENSITIVE).matcher(rl);
//        	if(mat.find()) {
//        		num_bots++;
////        		System.out.println("Bot");
//        	}
//        	if(num_lines%10000==0) System.out.print(".");
//        }
//		br.close();
//		in.close();
//		System.out.println("End.\nNumber of Lines: "+num_lines+"\nBots found: "+num_bots);
//		System.out.println("Exec. Time.: "+(LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3))-timenow));
		/*String regex_date = "(\\d{1,2}\\/(\\w{3}|\\d{1,2})\\/\\d{4})";
		String regex_time = "(\\d{1,2}:\\d{1,2}:\\d{1,2})";
		String regex_gmt = "([-+]{1}\\d{4})";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/user/bigdata/access.log");
		FSDataInputStream in = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String rl;
		String date = null,time = null,gmt = null;
		Long seconds;
		List<Long> sessions = new ArrayList<>();
		
		long  max = 0, min = 0;
		boolean first = true;
		while((rl=br.readLine())!=null) {
			Matcher mat1 = Pattern.compile(regex_date).matcher(rl);
			Matcher mat2 = Pattern.compile(regex_time).matcher(rl);
			int groupsize = mat1.groupCount();
			if(mat1.find()) {
				date = mat1.group(1);
			}
			if(mat2.find()) {
				time = mat2.group(1);
			}
			try {
				String date_format = date+" "+time;
				seconds = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss",Locale.ENGLISH).parse(date_format).getTime();				
				int epoch_size = sessions.size();
				if(epoch_size==0) {
					System.out.println(date_format+" -> Add a: "+seconds);
					sessions.add(seconds);
				} else {
					for(int i=0;i<epoch_size;i++) {
						Long s = sessions.get(i);
						if(seconds<=s) {
							System.out.println(date_format+" -> Add b: "+seconds);
							sessions.add(i, seconds);
							break;
						} else if(i==epoch_size-1) {
							System.out.println(date_format+" -> Add a: "+seconds);
							sessions.add(seconds);
							break;
						}
					}
				}
			} catch (ParseException e) {
				System.out.println("No epoch found");
				break;
			}
		}
		long last_sec = 0;
		int session_sum = 0;
		int session_total = 0;
		long sessions_median = 0;
		int sessions_qtd = 1;
		long session_time = 0;
		int sessions_total = 0;
		boolean newsession = true;
		for(Long sec : sessions) {
			long dif = 0;
			if(newsession) {
				last_sec = sec;
				session_total++;
				newsession = false;
			} else {
				dif = (sec-last_sec)/1000;
				if(dif>=900) {
					session_time += session_sum;
					sessions_total += session_total;
					sessions_median += session_sum/session_total;
					sessions_qtd++;
					session_sum = 0;
					session_total = 0;
					newsession = true;
				} else {
//					session_time += dif;
					last_sec = sec;
					session_sum += dif;
					session_total++;
					newsession = false;
				}
			}
			System.out.println(sec+" - "+new Date(sec).toString());
		}
		sessions_median = sessions_median/sessions_qtd;
		System.out.println("Requisições: "+sessions_total);
		System.out.println("Tempo de sessão total:\t"+session_time);
		System.out.println("Tempo de sessão médio:\t"+session_time/sessions_total);
		System.out.println("Tempo médio por sessão:\t"+sessions_median);
		System.out.println("Quantidade de sessões:\t"+sessions_qtd);*/
	}
}