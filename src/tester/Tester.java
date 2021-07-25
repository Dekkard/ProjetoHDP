package tester;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;

import model.WebLog;
import resources.RegexMatch;
import resources.Resources;
import resources.Setup;


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
		WebLog wl = new WebLog("\"192.168.4.25 - - [22/Dec/2016:16:30:52 +0300] \"POST /administrator/index.php/./.. HTTP/1.1\" 303 382 \"http://192.168.4.161/DVWA\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.21 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.21\"\"");
		System.out.println(wl.toString("\n"));
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
		Date date = null;
		try {
			date = new SimpleDateFormat("DD/MMM/yyyy:HH:mm:ss").parse("22/Jan/2019:03:58:06");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		System.out.println(date.getTime());
	}
}