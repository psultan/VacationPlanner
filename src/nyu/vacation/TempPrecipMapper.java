
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempPrecipMapper extends Mapper<LongWritable, Text, Text, Text> {



	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		//char[] Prcp;	
		int val =0;
		int i=1;
		while(line.contains("  ")){
		line=line.replaceAll("  ", " ");
		}
		String[] tokens = line.split(" ");
		int finalDate=0;
		int date = Integer.parseInt(tokens[0].substring(15,17)); 
		switch (date){
		case 1:date=0;break;
		case 2:date=31;break;
		case 3:date=59;break;
		case 4:date=90;break;
		case 5:date=120;break;
		case 6:date=151;break;
		case 7:date=181;break;
		case 8:date=212;break;
		case 9:date=243;break;
		case 10:date=273;break;
		case 11:date=304;break;
		case 12:date=334;break;
		
		}
		int day=1;
		if (tokens[0].substring(17, 21).equals("PRCP"))
		{   
		while(i<tokens.length)
		{finalDate =date+day;
		
		//	=tokens[i];
			if (finalDate>365)
				System.out.println("date= "+date+"Date extracted = "+ Integer.parseInt(tokens[0].substring(11,17)));
			day=day+1;
			if(!tokens[i].contains("-9999"))
			 context.write(new Text(Integer.toString(finalDate)),new Text("PR"+tokens[i]));
			 i+=2;
		}
		}
		else if(tokens[0].substring(17, 21).equals("TMAX"))
		{
		while(i<tokens.length)
		{finalDate =date+day;
		//	=tokens[i];
		
			day=day+1;
			if(!tokens[i].contains("-9999"))
			 context.write(new Text(Integer.toString(finalDate)),new Text("MA"+tokens[i]));
			 i+=2;
		}
		}
		else if (tokens[0].substring(17, 21).equals("TMIN"))
		{
		while(i<tokens.length)
		{finalDate =date+day;
		//	=tokens[i];
		
			day=day+1;
			if(!tokens[i].contains("-9999"))
			 context.write(new Text(Integer.toString(finalDate)),new Text("MI"+tokens[i]));
			 i+=2;
		}
		}
	 
	  }
	}