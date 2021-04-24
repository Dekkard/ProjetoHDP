package model;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebLog {
	private static String DELIMITER = ", \"[]";
	
	private static String REGEX_IP = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
	private static String REGEX_DATE = "((\\d{1,2})\\/(\\w+|\\d{1,2})\\/(\\d{4}))";
	private static String REGEX_TIME = ":((\\d{1,2}):(\\d{1,2}):(\\d{1,2}))" ;
	private static String REGEX_CODE = "(\\d{3})";
	private static String REGEX_URL = "((\\/\\w+([.|%]\\w+)?|\\/[.|%]*)*)";
	private static String REGEX_METHOD = "(GET|POST|PUT|DELETE)";
	
	private String ip;
	private String date;
	private String time;
	private Integer code;
	private String url;
	private String method;
	private Integer seconds;
	
	public WebLog() {}
	
	public WebLog(String line) {
		StringTokenizer st = new StringTokenizer(line,DELIMITER);
		while(st.hasMoreTokens()) {
			String word = st.nextToken();
			if(word.matches(REGEX_IP)) {
				setIp(word);
//				this.ip = word;
			} else if(word.matches(REGEX_DATE+REGEX_TIME)) {
				setDate(word);
//				this.date = word;
				setTime(word);
			} else if(word.matches(REGEX_DATE)) {
				setDate(word);
			} else if(word.matches(REGEX_TIME)) {
				setTime(word);
			} /*else if(word.matches("([-+](\\d{4}))")) {

			}*/ else if(word.matches(REGEX_METHOD)) {
				setMethod(word);
//				this.method = word;
			} /*else if(word.matches("(HTTP/(\\d+)\\.(\\d+))")) {
				
			}*/ else if(word.matches(REGEX_CODE)) {
				setCode(word);
//				this.code = Integer.parseInt(word);
			} else if(word.matches(REGEX_URL)) {
				setUrl(word);
//				this.url = word;
			}
		}
	}

	private static Integer timeToSec(int hour, int min, int sec) {
		return hour*3600+min*60+sec;
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String line) {
		Matcher mat = Pattern.compile(REGEX_IP).matcher(line);
		if(mat.find()) {
			this.ip = mat.group(1);
		}	
	}

	public String getDate() {
		return date;
	}

	public void setDate(String line) {
		Matcher mat = Pattern.compile(REGEX_DATE).matcher(line);
		if(mat.find()) {
			this.date = mat.group(1);
		}
	}
	
	public String getTime() {
		return time;
	}

	public void setTime(String line) {
		Matcher mat = Pattern.compile(REGEX_TIME).matcher(line);
		if(mat.find()) {
			this.time = mat.group(1);
			this.seconds = timeToSec(Integer.parseInt(mat.group(2)), Integer.parseInt(mat.group(3)), Integer.parseInt(mat.group(4)));
		}
	}

	public Integer getSeconds() {
		return seconds;
	}
	
	public String getSeconds(String suf) {
		return seconds+suf;
	}
	
	public Integer getCode() {
		return code;
	}
	
	public void setCode(String line) {
		if(code == null) {
			Matcher mat = Pattern.compile(REGEX_CODE).matcher(line);
			if(mat.find()) {
				this.code = Integer.parseInt(mat.group(1));
			}
		}
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String line) {
		Matcher mat = Pattern.compile("(HTTP\\/\\d+\\.\\d+)|"+REGEX_URL).matcher(line);
		if(mat.find()) {
			this.url = mat.group(2);
		}
	}
	
	public String getMethod() {
		return method;
	}

	public void setMethod(String line) {
		Matcher mat = Pattern.compile(REGEX_METHOD).matcher(line);
		if(mat.find()) {
			this.method = mat.group(1);
		}
	}
	
	public String toString() {
		return getIp()+" "+getDate()+" "+getTime()+" "+getSeconds("s")+" "+getCode()+" "+getUrl()+" "+getMethod();
	}
	
	public String toString(String s) {
		return getIp()+s+getDate()+s+getTime()+s+getSeconds("s")+s+getCode()+s+getUrl()+s+getMethod();
	}
	
	public Integer codeStatus() {
		int c = getCode();
		if(c >= 100 && c < 200) {
//			return "Info";
			return 1;
		} else if(c >= 200 && c < 300) {
//			return "Success";
			return 2;
		} else if(c >= 300 && c < 400) {
//			return "Redirect";
			return 3;
		} else if(c >= 400 && c < 500) {
//			return "Error - Client";
			return 4;
		} else if(c >= 500 && c < 600) {
//			return "Error - Server";
			return 5;
		} else {
			return null;
		}
	}
}
