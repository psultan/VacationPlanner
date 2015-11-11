
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TempPrecipReducer extends Reducer<Text, Text, Text, Text> {
	  @Override
	  public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
	    String Precp;
	    int AvgTmp=0;
	    int tmp=0;
		  for (org.w3c.dom.Text value : values) {
	    	if(value.toString().contains("PR"))
	    	{
	    		Precp = value.toString().substring(2, value.getLength());
	    	}
	    	else
	    	{
	    		if(AvgTmp==0)
	    			AvgTmp=Integer.parseInt(value.toString());
	    		else 
	    			AvgTmp=(AvgTmp+Integer.parseInt(value.toString()))/2;
	    		//Precp = value.toString().substring(2, value.getLength());
	    	}
	      
	    }
	    context.write(key, "Precipitation= "+Precp+"Average temperature = "+AvgTmp);
	  }
}