package org.nyu.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public abstract class SevereRunner {

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
	    Configuration conf = new Configuration();
	    conf.set("FIPS", "36001 36002 36003");
	    
		Job job = new Job(conf, "SevereMapper");
	    
	    job.setJarByClass(SevereRunner.class);
	    job.setJobName("SevereMapper");

	    FileInputFormat.addInputPath(job, new Path("/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/sample/Storm*"));
	    FileOutputFormat.setOutputPath(job, new Path("/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/output"));
	    
	    job.setMapperClass(SevereMapper.class);
	    job.setReducerClass(SevereReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
