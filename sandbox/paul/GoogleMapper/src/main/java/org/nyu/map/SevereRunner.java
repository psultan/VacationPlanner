package org.nyu.map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

public abstract class SevereRunner {
	
	/*
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
	    Configuration conf = new Configuration();
	    conf.set("FIPS", "39133 17099 31055 08075 49019 32003 06019");
		Job job = new Job(conf, "SevereMapper");
	    
	    job.setJarByClass(SevereRunner.class);
	    job.setJobName("SevereMapper");

		if (args.length != 2) {
		      args = new String[2];
		      args[0] = "s3n://severeweather/StormEvents_details-ftp_v1.0_d1950_c20150826.csv";
		      args[1] = "s3n://severeweather/results";
		}
		System.out.println("Reading:"+args[0]);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(SevereMapper.class);
	    job.setReducerClass(SevereReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    long startTime = System.nanoTime();
	    boolean result = job.waitForCompletion(true);
	    long endTime =System.nanoTime();
	    System.out.println("Took "+(double)(endTime-startTime)/60000000000.0+" minutes");
	    

	    System.exit(result? 0 : 1);
	}
	*/
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
        try {
	        pigServer.registerQuery("A = load '" + "/user/cloudera/severeresult" + "' using TextLoader();");
	        pigServer.registerQuery("B = foreach A generate $0 as id;");
	        pigServer.store("B", "finalresult");
	    } 
	    catch (IOException e) {
	        e.printStackTrace();
	    }

		Path pt=new Path("hdfs://quickstart.cloudera:8020/user/cloudera/finalresult/part-m-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        try {
          String line;
          line=br.readLine();
          while (line != null){
            System.out.println(line);
            line = br.readLine();
          }
        } finally {
          br.close();
        }
	}

}
