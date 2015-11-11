
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempPrecipMapper extends Mapper<LongWritable, Text, Text, Text> {
{


	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		//char[] Prcp;	
		int val =0;
		int i=1;
		String[] tokens = line.split(" ");
		String date = tokens[0].substring(12,17); 
		int day=1;
		if (tokens[0].substring(18, 21)=="PRCP")
		{
		while(i<tokens.length)
		{date =date+Integer.toString(day);
		//	=tokens[i];
			i+=4;
			day=day+1;
			 context.write(new Text(date),new Text("PR"+tokens[i]));
		}
		}
		else if(tokens[0].substring(18, 21)=="TMAX")
		{
		while(i<tokens.length)
		{date =date+Integer.toString(day);
		//	=tokens[i];
			i+=4;
			day=day+1;
			 context.write(new Text(date),new Text("MA"+tokens[i]));
		}
		}
		else if (tokens[0].substring(18, 21)=="TMIN")
		{
		while(i<tokens.length)
		{date =date+Integer.toString(day);
		//	=tokens[i];
			i+=4;
			day=day+1;
			 context.write(new Text(date),new Text("MI"+tokens[i]));
		}
		}
	 
	  }
	}
