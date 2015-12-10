
package nyu.traffic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;

public class TrafficFatalityReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
		HashMap<String, int[]> scale = new HashMap<String, int[]>();
		scale.put("HarmEvent"       , new int[]{0,0} );
		scale.put("Fatalities"      , new int[]{0,0} );
		scale.put("DrunkDriving"    , new int[]{0,0} );
		 

	//build a cache since hadoop can only iterate over values once
	ArrayList<Text> cache = new ArrayList<Text>();
	for(Text value: values){
		cache.add(new Text(value));
		String[] stormtype = value.toString().split("_");
		//stormtype is last element in value
		int[] scalevalues = scale.get(stormtype[stormtype.length-1]);
		//increment scale tally for associated stormtype
		scalevalues[1]++;
	}
	
	//format output
	//startday, total, [stormtally_stormtype,]
	int total=0;
	List<String> tallies = new ArrayList<String>();
	for(String storm: scale.keySet()){
		int[] scalevalues = scale.get(storm);
		int typetotal=scalevalues[0]*scalevalues[1];
		if(typetotal>0){
			tallies.add(scalevalues[1]+"_"+storm);
			total+=typetotal;
		}
	}
	String startday = key.toString().substring(0,3);
	String totals = String.valueOf(total);
	
	//output with total
	//context.write(new Text(startday),new Text(StringUtils.leftPad(totals,totals.length(),'0')));
	
	//output with total and tallies
	//context.write(new Text(startday),new Text(StringUtils.leftPad(totals,totals.length(),'0')+"\t"+Joiner.on(" ").join(tallies)));
	
	//output with total and tallies and dates
	context.write(new Text(startday),new Text(StringUtils.leftPad(totals,totals.length(),'0')+"\t"+Joiner.on(" ").join(tallies)+" "+Joiner.on(" ").join(cache)));
  }
}
