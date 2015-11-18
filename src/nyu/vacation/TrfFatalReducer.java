
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TrfFatalReducer
extends Reducer <Text, Text, Text, Text>
{
	  @Override
	  
    public void reduce(Text key, Iterable<Text> values,  Context context)
    throws
    IOException, InterruptedException {
    String DRUNK_DR;
	    int velocity=0;
	    int blood_alcohol_conc=0;
        int MIACCyr=0;
		  
        for (org.w3c.dom.Text value : values)
        {
	    	if(value.toString().contains("ACCIDENT_DR"))
	    	{
	    		ACCIDENT = value.toString().substring(2, value.getLength());
	    	}
	    	
            else
	    	{
	    		if(velocity==0)
	    			velocity=Integer.parseInt(value.toString());
	    		else 
	    			velocity=(velocity+Integer.parseInt(value.toString()))/2;
	    	}
	      
	    }
	    context.write(key, "Accident Fatality= "+ACCIDENT+"Velocity = "+velocity);
	  }
}