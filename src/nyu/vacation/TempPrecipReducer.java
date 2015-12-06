
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TempPrecipReducer extends Reducer<Text, Text, Text, String> {
	  @Override
	  public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
	    String Precp="";
	    int idealTemp=0,tempPeriod=10,idealPrecipitation=0,preciPeriod=10,score=0;
	    int AvgTmp=0;
	    int tmp=0;
	    Pattern p =null;
		  for (Text value : values) {
	    	if(value.toString().contains("PR"))
	    	{
	    		Precp = value.toString().substring(2, value.getLength());
	    		//Precp.isEmpty();
	    	}
	    	else if(value.toString().contains("MI") || value.toString().contains("MA"))
	    	{
	    		if(AvgTmp==0 && value.toString().length()>3)
	    			AvgTmp=Integer.parseInt(value.toString().substring(3));
	    		else if(value.toString().length()>3 && AvgTmp!=0)
	    			AvgTmp=(AvgTmp+Integer.parseInt(value.toString().substring(3)))/2;
	    		
	    	}
	    	else
	    	{
	    		
	    		//Precp = value.toString().substring(2, value.getLength());
	    	}
	      
	    }
		  if(p.matches(".*[a-zA-Z]+.*", Precp))
			  Precp=Precp.substring(0,Precp.length()-1 );
		  if (!Precp.equals(""))
		  score =(((Integer.parseInt(Precp))/10-idealPrecipitation)/preciPeriod)+(AvgTmp/10-idealTemp)/tempPeriod;
	    context.write(key,"Score="+ Integer.toString(score));
	  }
}