import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrfFatalRun {
	
public static void main(String[] args)
    throws Exception
    { BasicConfigurator.configure();
        Configuration conf = new Configuration();
	    
		Job job = new Job(conf, "TrfFatalMapper");
	    
	    job.setJarByClass(TrfFatalRun.class);
	    job.setJobName("TrfFatalMapper");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(TrfFatalMapper.class);
	    job.setReducerClass(TrfFatalReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);


}
