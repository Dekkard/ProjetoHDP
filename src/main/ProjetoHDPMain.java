package main;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import kmeans.Kmeans;
import kmeans.Kmeans.KMapper;
import kmeans.Kmeans.KReducer;
import model.WebLog;

@SuppressWarnings("all")
public class ProjetoHDPMain {
	
	public static class ClasseMapper extends Mapper<Object, Text, Text, Text>{		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			WebLog wl = new WebLog(value.toString());
			if(!wl.getIp().isEmpty()) {
				context.write(new Text("Users"), new Text("1"));
				context.write(new Text(wl.getIp()), new Text( 1+";"+wl.getSeconds()+";"+wl.getMethod() ));
//				context.write(new Text(wl.getIp()+":Sessions"), new IntWritable(1));
//				context.write(new Text(wl.getIp()+":SessionsTime"), new IntWritable(wl.getSeconds()));

			}
		}
	}

	public static class ClasseReducer extends Reducer<Text,IntWritable,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(key.toString().matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})")) {
				int rslt1 = 0;
				int rslt2 = 0;
				int max = 0;
				int min = 0;
				int get = 0, put = 0, post = 0, del = 0;
				for(Text t : values) {
					StringTokenizer st = new StringTokenizer(t.toString(),";");
					int v;
					String m;
					v = Integer.parseInt(st.nextToken());
					rslt1 += v;
					v = Integer.parseInt(st.nextToken());
					if(min == 0 || v < min) {
						min = v;
					}
					if(max == 0 || v > max) {
						max = v;
					}
					m = st.nextToken();
					if(m.equals("GET")) {
						get += 1;
					} else if(m.equals("PUT")) {
						put += 1;
					} else if(m.equals("POST")) {
						post += 1;
					} else if(m.equals("DELETE")) {
						del += 1;
					}
				}
				rslt2 = max - min;
				context.write(key, new Text(rslt1+";"+rslt2+";"+get+";"+put+";"+post+";"+del));
			}
			/*if(key.toString().matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})(:Sessions)")) {
				for(IntWritable v: values) {
					rslt += v.get();
				}
			}
			if(key.toString().matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})(:SessionsTime)")) {
				for(IntWritable v: values) {
					if(min == 0 || v.get() < min) {
						min = v.get();
					}
					if(max == 0 || v.get() > max) {
						max = v.get();
					}
				}
				rslt = max - min;
			}*/
			if(key.toString().equals("Users")) {
				int rslt = 0;
				for(Text t: values) {
					rslt += Integer.parseInt(t.toString());
				}
				context.write(key, new Text(String.valueOf(rslt)));
			}
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: ProjetoHDPMain <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    
		Job job1 = Job.getInstance(conf, "Teste Fase 1");
		
		/*Classe no qual o Job será baseado*/
		job1.setJarByClass(ProjetoHDPMain.class);

		/*Classe mapeadora*/
	    job1.setMapperClass(ClasseMapper.class);
	    /*Classe combinadora*/
//	    job1.setCombinerClass(Combiner.class);
	    /*Classe reduzidora*/
	    job1.setReducerClass(ClasseReducer.class);

//	    job.setInputFormatClass();
//	    job.setOutputFormatClass();
	    /*Classes que lidam com os tipos de chave-valor*/
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
		
		/*Arquivos de entrada*/
	    // Aceita mais de um diretório ou arquivo [?]
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
	    }
		/*Arquivo de saida*/
		FileOutputFormat.setOutputPath(job1, new Path("MiddleMan"));
		
		job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "Teste Fase 2");
		
		job2.setJarByClass(Kmeans.class);
	    job2.setMapperClass(KMapper.class);
	    job2.setReducerClass(KReducer.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job2, new Path("MiddleMan"));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
	    
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
