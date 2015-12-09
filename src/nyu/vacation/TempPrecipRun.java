import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class TempPrecipRun {
	
	static List<String> data ;
	static List<String> combinedfiles=new ArrayList<String>();
	static List<String> inter;
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
	    Configuration conf = new Configuration();
	    String interimFile ="/home/cloudera/workspace/RealTimeBigData/Input/interim.dly";
		Job job = new Job(conf, "TempPrecipMapper");
	    
	    job.setJarByClass(TempPrecipRun.class);
	    job.setJobName("TempPrecipMapper");
	    String FileName =args[0];
		
		 data =readFile(FileName);
		String [] filesToread=new String[14];
		String files;
		double[][] geoCoords={{41.242090000000005, -81.05753},{41.376400000000004, -88.60452000000001},{41.22317, -96.04826000000001},{41.22241, -103.45479},{41.29525, -110.77547000000001}, {40.87344, -117.24944}, {37.7749901, -122.4194926}};
		//double[][] geoCoords={{41.243900000000004, -81.08354000000001}, {41.375780000000006, -88.656},{41.19908, -96.09541}, {40.44715, -103.31071000000001},{38.944370000000006, -109.64305},{36.366440000000004, -114.90073000000001},{36.7782392, -119.4179254}};
	for (int i =0;i<14;i=i+2)
	{
		int j=0;
		
		filesToread[i]=getMinDistance(geoCoords[j][0],geoCoords[j][1],"PRCP");
		filesToread[i+1]=getMinDistance(geoCoords[j][0],geoCoords[j][1],"TMAX");
		files="/home/cloudera/workspace/RealTimeBigData/Input/"+filesToread[i]+ ".dly";
		inter = readFile(files);
		combinedfiles.addAll(inter);
		System.out.println(files);
		files="/home/cloudera/workspace/RealTimeBigData/Input/"+filesToread[i+1]+ ".dly";
		inter = readFile(files);
		combinedfiles.addAll(inter);
		System.out.println(files);
	}
	File f = new File(interimFile);
	FileWriter fr = new FileWriter(f);
	BufferedWriter br  = new BufferedWriter(fr);
	for (int i=0;i<combinedfiles.size();i++)
	{
		 br.write(combinedfiles.get(i));
	}

	br.close();

		
		FileInputFormat.addInputPath(job, new Path(interimFile));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(TempPrecipMapper.class);
	    job.setReducerClass(TempPrecipReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);


}
	private static String getMinDistance(double x,double y,String val)
	{
		String bestMatch="";
		boolean initialised=false;
		double minDistance =0.0;
		for (int i=0;i<data.size();i++) {
		    String line= data.get(i);
		    while(line.contains("  ")){
				line=line.replaceAll("  ", " ");
				}
		    String[] value=line.split(" ");
		    double valx = Double.parseDouble(value[1]);
		    double valy = Double.parseDouble(value[2]);
		    String fileType=value[3];
		    if(fileType.equals(val)){
		    double dist = Math.pow((Math.pow((valx-x),2.0)+Math.pow((valy-y),2.0)),0.5);
		    if (!initialised) 
		    {
		    	minDistance =dist;
		    	bestMatch = value[0];
		    	initialised=true;
		    }
		    else if(minDistance > dist)
		    {
		    		minDistance=dist;
		    		bestMatch = value[0];
		   	}
		    }
		}
		return bestMatch;
	}
	private  static List<String> readFile(String pathname)  {

	    File file = new File(pathname);
	    StringBuilder fileContents = new StringBuilder((int)file.length());
	    List<String> datastructsure=new ArrayList<String>();
	    String s ="";
	    

	    try {
	    	Scanner scanner = new Scanner(file);
		    String lineSeparator = System.getProperty("line.separator");
	        while(scanner.hasNextLine()) {        
	           // fileContents.append(scanner.nextLine() + lineSeparator);
	            datastructsure.add(scanner.nextLine() + lineSeparator);
	        }
	        scanner.close();
	       // s=fileContents.toString();
	        
	    }
	    catch (Exception e )
	    {
	    	System.out.println("ERROR reading file");
	    }finally {
	    	return datastructsure;
	    }
	}
}