package resources;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Resources {
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
	public static <T> void appendFile(Configuration conf, String line, String filename, String path) throws IllegalArgumentException, IOException {
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
		int k = Integer.parseInt(conf.get(Setup.K_CLUSTER_SIZE));
		int d = Integer.parseInt(conf.get(Setup.D_PARAM_SIZE));
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
}
