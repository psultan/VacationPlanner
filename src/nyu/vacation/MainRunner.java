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
		 * arg4 severe weather data
		 * arg5 severe weather output
		 * arg6 temp/prec data
		 * arg7 temp/prec output
		 * arg8 traffic data
		 * arg9 traffic output
		 * 
		 * arg10 final output
		 */
		long startTime = System.nanoTime();
		if (args.length != 10) {
			  //default args
		      args = new String[11];
		      args[0] = "NY";
		      args[1] = "California";
		      args[2] = "7";
		      args[3] = "75";
		      
		      //args[4] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/Storm*";
		      //args[5] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/results";
		      args[4] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/Storm*";
		      args[5] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/results";
		      
		      args[10] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/results2";
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
	    PigServer pigServer = new PigServer(ExecType.LOCAL);
        try {
        	pigServer.registerQuery("A = LOAD '"+args[5]+"' USING PigStorage() as (day:double, total:double);");
        	pigServer.registerQuery("B = ORDER A BY total;");
        	pigServer.registerQuery("C = ORDER A BY total DESC;");
        	pigServer.registerQuery("min = LIMIT B 1;");
        	pigServer.registerQuery("max = LIMIT C 1;");
        	pigServer.registerQuery("scaled = FOREACH A GENERATE $0, (10-1)/(max.total-min.total)*($1-max.total)+10;");
        	pigServer.registerQuery("scaled = ORDER scaled BY $1;");
	        pigServer.store("scaled", args[10]);
	    } 
	    catch (IOException e) {
	        e.printStackTrace();
	    }
        
	    long endTime =System.nanoTime();
	    System.out.println("Took "+(double)(endTime-startTime)/60000000000.0+" minutes");
	    
	    System.exit(result? 0 : 1);
	}

}
