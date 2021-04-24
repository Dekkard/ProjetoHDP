package teste;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;

import model.WebLog;
import resources.RegexMatch;

public class Teste {
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
	
	public static void main(String[] args) {
//		wordRecon1(args);
//		wordRecon2(args);
//		wordRecon3(args);
//		listMaker();
		compareTest();
	}
}