package tester;

import java.text.ParseException;
import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import resources.Resources;

@SuppressWarnings("all")
public class WebLog2 {
	private static String DELIMITER = ", \"[]";
	private static String REGEX_IP = Resources.REGEX_IP;
	private static String REGEX_DATE = "(\\d{1,2}\\/(\\w{3}|\\d{1,2})\\/\\d{4})";;
	private static String REGEX_TIME = "(\\d{2}:\\d{2}:\\d{2})";
	private static String REGEX_GMT = Resources.REGEX_GMT;
	private static String REGEX_CODE = "((\\d{3}) (\\d+))";
	private static String REGEX_BYTES = Resources.REGEX_BYTES;
	private static String REGEX_URL_REF = Resources.REGEX_URL_REF;
	private static String REGEX_URL = Resources.REGEX_URL;
	private static String REGEX_PROTOCOL = Resources.REGEX_PROTOCOL;
	private static String REGEX_METHOD = "((GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH) ((\\/\\S+)+))"; //Resources.REGEX_METHOD;
	private String ip;
	private String date;
	private String time;
	private String gmt;
	private Integer code;
	private String url;
	private String urlRef;
	private String method;
	private Long seconds;
	private String protocol;
	private Integer bytes;
	private String domain;
	public WebLog2() {}
	
	public WebLog2(String line, String regw, String regu) {
		Matcher mat = Pattern.compile(REGEX_IP+".*"+REGEX_DATE+".*"+REGEX_TIME+".*"+REGEX_METHOD+".*"+REGEX_PROTOCOL+".*"+REGEX_CODE).matcher(line);
		if(mat.find()) {
			this.ip = mat.group(1);
			this.date = mat.group(2);
			this.time = mat.group(4);
			this.method = mat.group(6);
			String url = mat.group(7);
			Matcher mat1 = Pattern.compile(regu,Pattern.CASE_INSENSITIVE).matcher(url);
			if(!mat1.find() || regu.isEmpty()) {
				mat1 = Pattern.compile(regw,Pattern.CASE_INSENSITIVE).matcher(url);
				if(mat1.find() || regw.isEmpty()) {
					this.url = url;
				}
			}
			this.protocol = mat.group(9);
			this.code = Integer.parseInt(mat.group(12));
			this.bytes = Integer.parseInt(mat.group(13));
			if(this.date!=null && this.time!=null && this.gmt!=null) {
				try {
					String full_date = this.date+" "+this.time;
					this.seconds = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss",Locale.ENGLISH).parse(full_date).getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}
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
		}
	}
	public String getGMT() {
		return gmt;
	}
	public void setGMT(String line) {
		this.gmt = line;
	}
	public Long getSeconds() {
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
		} else {
			setBytes(line);
		}
	}
	public void setUrl(String line, String regw, String regu) {
		Matcher mat1 = Pattern.compile(regu,Pattern.CASE_INSENSITIVE).matcher(line);
		if(!mat1.find()) {
			mat1 = Pattern.compile(regw,Pattern.CASE_INSENSITIVE).matcher(line);
			if(mat1.find()) {
				this.url = line;
			}
		}
	}
	public void setUrlRef(String line) {
		Matcher mat = Pattern.compile(REGEX_URL_REF).matcher(line);
		if(mat.find()) {
			this.urlRef = mat.group(1);
			this.domain = mat.group(4);
		}
	}
	public String getDomain() {
		return this.domain;
	}
	public String getUrl() {
		return url;
	}
	public String getUrlRef() {
		return urlRef;
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
	public String toString(boolean header) {
		if(header) return "IP: "+getIp()+" "+"Date: "+getDate()+" "+"Time: "+getTime()+" "+"Code: "+getCode()+" "+"URL: "+getUrl()+" "+"Method: "+getMethod()+" "+"Protocol: "+getProtocol()+" "+"Bytes: "+getBytes();
		else return getIp()+" "+getDate()+" "+getTime()+" "+getCode()+" "+getUrl()+" "+getMethod()+" "+getProtocol()+" "+getBytes();
	}
	public String toString(boolean header, String s) {
		if(header) return "IP: "+getIp()+s+"Date: "+getDate()+s+"Time: "+getTime()+s+"Code: "+getCode()+s+"URL: "+getUrl()+s+"Method: "+getMethod()+s+"Protocol: "+getProtocol()+s+"Bytes:  "+getBytes();
		else return getIp()+s+getDate()+s+getTime()+s+getCode()+s+getUrl()+s+getMethod()+s+getProtocol()+s+getBytes();
	}
	public Integer codeStatus() {
		int c = getCode();
		if(c >= 100 && c < 200) {
			return 1;
		} else if(c >= 200 && c < 300) {
			return 2;
		} else if(c >= 300 && c < 400) {
			return 3;
		} else if(c >= 400 && c < 500) {
			return 4;
		} else if(c >= 500 && c < 600) {
			return 5;
		} else {
			return null;
		}
	}
	public String getProtocol() {
		return protocol;
	}
	public void setProtocol(String line) {
		Matcher mat = Pattern.compile(REGEX_PROTOCOL).matcher(line);
		if(mat.find()) {
			this.protocol = mat.group(1);
		}
	}
	public Integer getBytes() {
		return bytes;
	}
	public void setBytes(String line) {
		this.bytes = Integer.parseInt(line);
	}
}
