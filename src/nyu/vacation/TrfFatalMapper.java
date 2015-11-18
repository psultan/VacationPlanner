
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrfFatalMapper
extends Mapper <LongWritable, Text, Text, Text>
{
{
@Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
          int val =0;
          int i=1;
		String[] tokens = line.split(" ");
		String date = tokens[0].substring(8,13);
		
          int year=1;
		
          if (tokens[0].substring(14, 28)=="ACCIDENT")
    {
		while(i<tokens.length)
		
        {
            date =date+Integer.toString(year);
            i+=4;
			year=year+1;
			 context.write(new Text(date),new Text("ACCIDENT_DR"+tokens[i]));
		}
		}
	
	 
	  }
	}
