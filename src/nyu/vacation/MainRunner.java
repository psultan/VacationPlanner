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


public abstract class MainRunner {

	public static void main(String[] args) throws Exception {
		/* arg0 start location
		 * arg1 end location
		 * arg2 duration
		 * arg3 severe weather data
		 * arg4 severe weather output
		 * arg5 temp/prec data
		 * arg6 temp/prec output
		 * arg7 traffic data
		 * arg8 traffic output
		 * arg9 final output
		 */
		if (args.length != 10) {
		      args = new String[10];
		      args[0] = "NY";
		      args[1] = "California";
		      args[2] = "7";
		      //args[3] = "s3n://severeweather/StormEvents_details-ftp_v1.0_d1950_c20150826.csv";
		      //args[4] = "s3n://severeweather/results";
		      args[3] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/Storm*";
		      args[4] = "/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/result";
		}
		BasicConfigurator.configure();
		Configuration conf = new Configuration();
		
		Map geodata = new Map();
		double[][] latlngs = geodata.getLatLng(args[0], args[1], args[2]);
		List<String> FIPS = geodata.getFIPS(latlngs);
	    conf.set("FIPS", Arrays.toString(FIPS.toArray()));
	    
	    //severe mapreduce job
	    Job severeJob = new Job(conf, "SevereMapper");
	    severeJob.setJarByClass(SevereRunner.class);
	    severeJob.setJobName("SevereMapper");
	    FileInputFormat.addInputPath(severeJob, new Path(args[3]));
	    FileOutputFormat.setOutputPath(severeJob, new Path(args[4]));
	    severeJob.setMapperClass(SevereMapper.class);
	    severeJob.setReducerClass(SevereReducer.class);
	    severeJob.setMapOutputKeyClass(Text.class);
	    severeJob.setMapOutputValueClass(Text.class);
	    severeJob.setOutputKeyClass(IntWritable.class);
	    severeJob.setOutputValueClass(Text.class);
	    
	    
	    
	    //temp/precip mapreduce job
	    
	    
	    
	    //traffic mapreduce job
	    
	    
	    
	    
	    //final mapreduce (pig)
	    PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
        try {
	        pigServer.registerQuery("A = load '" + "/user/cloudera/severeresult" + "' using TextLoader();");
	        pigServer.registerQuery("B = foreach A generate $0 as id;");
	        pigServer.store("B", "finalresult");
	    } 
	    catch (IOException e) {
	        e.printStackTrace();
	    }

        
        //read pig output
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
	    
	    
	    long startTime = System.nanoTime();
	    boolean result = severeJob.waitForCompletion(true);
	    long endTime =System.nanoTime();
	    System.out.println("Took "+(double)(endTime-startTime)/60000000000.0+" minutes");
	    
	    System.exit(result? 0 : 1);
	}

}
