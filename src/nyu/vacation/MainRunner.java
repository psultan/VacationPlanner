package nyu.vacation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

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

import com.google.api.client.repackaged.com.google.common.base.Joiner;


public abstract class MainRunner {

	public static void main(String[] args) throws Exception {
		/* arg0 start location
		 * arg1 end location
		 * arg2 duration
		 * arg3 desired temperature
		 * arg4 severe weather data (paul)
		 * arg5 severe weather output (paul)
		 * arg6 temp/prec data (ankur)
		 * arg7 temp/prec output (ankur)
		 * arg8 traffic data (vipul)
		 * arg9 traffic output (vipul)
		 * 
		 * arg10 final output
		 */
		long startTime = System.nanoTime();
		if (args.length != 10) {
			  //default args
		      args = new String[11];
		      args[0] = "Maine";
		      args[1] = "Florida";
		      args[2] = "10";
		      args[3] = "75";
		      
		      //args[4] = "s3n://severeweather/StormEvents_details-ftp_v1.0_d1950_c20150826.csv";
		      //args[5] = "s3n://severeweather/results";
		      args[4] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/Storm*";
		      args[5] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/severeresults";
		      
		      args[7] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/weatherresults";
		      
		      args[9] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/trafficresults";
		      
		      args[10] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/results";
		}
		BasicConfigurator.configure();
		Configuration conf = new Configuration();
		
		Map geodata = new Map();
		double[][] latlngs = geodata.getLatLng(args[0], args[1], args[2]);
		List<String> FIPS = geodata.getFIPS(latlngs);
		System.out.println(Joiner.on(" ").join(FIPS.toArray()));
	    conf.set("FIPS", Joiner.on(" ").join(FIPS.toArray()));
	    
	    //severe mapreduce job
	    Job severeJob = new Job(conf, "SevereMapper");
	    severeJob.setJarByClass(MainRunner.class);
	    severeJob.setJobName("SevereMapper");
	    FileInputFormat.addInputPath(severeJob, new Path(args[4]));
	    FileOutputFormat.setOutputPath(severeJob, new Path(args[5]));
	    severeJob.setMapperClass(SevereMapper.class);
	    severeJob.setReducerClass(SevereReducer.class);
	    severeJob.setMapOutputKeyClass(Text.class);
	    severeJob.setMapOutputValueClass(Text.class);
	    severeJob.setOutputKeyClass(IntWritable.class);
	    severeJob.setOutputValueClass(Text.class);
	    
	    
	    //temp/precip mapreduce job
	    
	    
	    
	    //traffic mapreduce job
	    
	    
	    
	    boolean result = severeJob.waitForCompletion(true);
	    
	    //final mapreduce (pig) 
	    //scale each mapred to 1-100 scale, weight mapred as .5severeweather + .3weather + .2traffic
	    //change to ExecType.MapReduce when running on hadoop
	    PigServer pigServer = new PigServer(ExecType.LOCAL);
        try {
        	pigServer.registerQuery("severe = LOAD '"+args[5]+"' USING PigStorage() as (day:double, total:double);");
        	pigServer.registerQuery("weather = LOAD '"+args[7]+"' USING PigStorage() as (day:double, total:double);");
        	pigServer.registerQuery("traffic = LOAD '"+args[9]+"' USING PigStorage() as (day:double, total:double);");
        	
        	pigServer.registerQuery("severeup = ORDER severe BY total;");
        	pigServer.registerQuery("severedown = ORDER severe BY total DESC;");
        	pigServer.registerQuery("severemin = LIMIT severeup 1;");
        	pigServer.registerQuery("severemax = LIMIT severedown 1;");
        	pigServer.registerQuery("severescaled = FOREACH severe GENERATE $0, (100-1)/(severemax.total-severemin.total)*($1-severemax.total)+100;");
        	pigServer.registerQuery("severescaled = ORDER severescaled BY $1;");
        	
        	/*
        	pigServer.registerQuery("weatherup = ORDER weather BY total;");
        	pigServer.registerQuery("weatherdown = ORDER weather BY total DESC;");
        	pigServer.registerQuery("weathermin = LIMIT weatherup 1;");
        	pigServer.registerQuery("weathermax = LIMIT weatherdown 1;");
        	pigServer.registerQuery("weatherscaled = FOREACH weather GENERATE $0, (100-1)/(weathermax.total-weathermin.total)*($1-weathermax.total)+100;");
        	pigServer.registerQuery("weatherscaled = ORDER weatherscaled BY $1;");
        	
        	pigServer.registerQuery("trafficup = ORDER traffic BY total;");
        	pigServer.registerQuery("trafficdown = ORDER traffic BY total DESC;");
        	pigServer.registerQuery("trafficmin = LIMIT trafficup 1;");
        	pigServer.registerQuery("trafficmax = LIMIT trafficdown 1;");
        	pigServer.registerQuery("trafficscaled = FOREACH traffic GENERATE $0, (100-1)/(trafficmax.total-trafficmin.total)*($1-trafficmax.total)+100;");
        	pigServer.registerQuery("trafficscaled = ORDER trafficscaled BY $1;");
        	
        	pigServer.registerQuery("joined = JOIN severescaled BY $0, weatherscaled BY $0, trafficscaled BY $0;");
        	pigServer.registerQuery("total = FOREACH A GENERATE $0,.5*$1+.3*$2+.2*$3;");
        	
	        pigServer.store("total", args[10]);
	        */
        	
        	pigServer.store("severescaled", args[10]);
	    } 
	    catch (IOException e) {
	        e.printStackTrace();
	    }

        
	    long endTime =System.nanoTime();
	    System.out.println("Took "+(double)(endTime-startTime)/60000000000.0+" minutes");
	    
	    System.exit(result? 0 : 1);
	}

}
