import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class TempPrecipRun {
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
	    Configuration conf = new Configuration();
	    
		Job job = new Job(conf, "TempPrecipMapper");
	    
	    job.setJarByClass(TempPrecipRun.class);
	    job.setJobName("TempPrecipMapper");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(TempPrecipMapper.class);
	    job.setReducerClass(TempPrecipReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);


}
