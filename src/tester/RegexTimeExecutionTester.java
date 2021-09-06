package tester;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import resources.Resources;
import resources.Resources.StoredMethodString;

@SuppressWarnings("all")
public class RegexTimeExecutionTester {
	public static void timedExec(FileSystem fs, int num_rep, Path path, String fname, StoredMethodString sm) throws FileNotFoundException, IOException {
		Long timenow, timenowinter;
		timenow = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
		for(int i=1;i<=num_rep;i++) {
			timenowinter = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3));
			System.out.print("\tComeço "+i+":");
			Resources.readFiles(fs, path, fname , sm);
			timenowinter = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3))-timenowinter;
			System.out.println(" Tempo Ex.: "+timenowinter+"s");
		}
		timenow = (LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(-3))-timenow)/num_rep;
		System.out.println("\t\tTempo Ex. médio: "+timenow+"s");	
	}
	public static void timedExec1(FileSystem fs, int num_exec, int num_rep, Path path, String fname) throws FileNotFoundException, IOException {
		for(int k=1;k<=num_exec;k++) {
			System.out.println("Tempo: Compilador Regex único:");
			timedExec(fs,num_rep,path,fname,(line)->{
				WebLog2 wl2 = new WebLog2(line,"","");
//				System.out.println(wl.toString());
			});
			System.out.println("Tempo: N-Compiladores Regex:");
			timedExec(fs,num_rep,path,fname,(line)->{
				WebLog1 wl1 = new WebLog1(line,"","");
//				System.out.println(wl.toString());
			});
		}
	}
	public static void timedExec2(FileSystem fs, int num_exec, int num_rep, Path path, String fname) throws FileNotFoundException, IOException {
		for(int k=1;k<=num_exec;k++) {
			System.out.println("Tempo: N-Compiladores Regex:");
			timedExec(fs,num_rep,path,fname,(line)->{
				WebLog1 wl1 = new WebLog1(line,"","");
//				System.out.println(wl.toString());
			});
			System.out.println("Tempo: Compilador Regex único:");
			timedExec(fs,num_rep,path,fname,(line)->{
				WebLog2 wl2 = new WebLog2(line,"","");
//				System.out.println(wl.toString());
			});
		}
	}
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.241.226.166:9001");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/user/bigdata");
		String fname = "access_sample5.log";
		Long timenow, timenowinter;
		/*System.out.println("Experimento 1 - 5 repeções, 3 execuções");
		System.out.println("1:");
		timedExec1(fs,5,3,path,fname);
		System.out.println("2:");
		timedExec2(fs,5,3,path,fname);
		System.out.println("Experimento 1 - 5 repeções, 5 execuções");
		System.out.println("1:");
		timedExec1(fs,5,5,path,fname);
		System.out.println("2:");
		timedExec2(fs,5,5,path,fname);
		System.out.println("Experimento 1 - 5 repeções, 8 execuções");
		System.out.println("1:");
		timedExec1(fs,5,8,path,fname);
		System.out.println("2:");
		timedExec2(fs,5,8,path,fname);*/
		timedExec(fs,8,path,fname,(line)->{
			WebLog3 wl3 = new WebLog3(line,"","");
		});
	}
}
