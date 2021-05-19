package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import resources.FilenameGen;
import resources.Setup;

public class ProjetoHDPMain {
//	scp -P 15487 ./ProjetoHDP.jar hadoop@192.168.1.5:~/Downloads/
//	scp -P 15487 ./ProjetoHDP.jar hadoop@192.168.1.6:~/Downloads/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path jobs_path = new Path("jobs_"+FilenameGen.dateGen(true,false));
	    if(!fs.exists(jobs_path)) fs.mkdirs(jobs_path);
	    String numJobsFiles = String.valueOf(fs.listStatus(jobs_path).length+1);
	    conf.set(Setup.JOB_PATH, jobs_path.getName()+"/job_"+"0".repeat(4-numJobsFiles.length())+numJobsFiles);
	    
	    args = KmeansMain.setConfArgs(conf,args);
		int res = ToolRunner.run(conf, new RegexReconMain(), args);
		if(res==0) fs.delete(new Path(conf.get(Setup.JOB_PATH)+"/data.var"),true);
		res = ToolRunner.run(conf, new KmeansMain(), args);
	    System.exit(res);
	}
}
