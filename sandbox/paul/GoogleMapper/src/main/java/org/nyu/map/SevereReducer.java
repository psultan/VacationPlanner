package org.nyu.map;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SevereReducer extends Reducer<Text, Text, Text, IntWritable> {
  @Override
  public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> scale = new HashMap<String, Integer>();
		scale.put("Astronomical Low Tide",1);
		scale.put("Avalanche",1);
		scale.put("Blizzard",1);
		scale.put("Coastal Flood",1);
		scale.put("Cold/Wind Chill",1);
		scale.put("Debris Flow",1);
		scale.put("Dense Fog",1);
		scale.put("Dense Smoke",1);
		scale.put("Drought",1);
		scale.put("Dust Devil",1);
		scale.put("Dust Storm",1);
		scale.put("Excessive Heat",1);
		scale.put("Extreme Cold/Wind Chill",1);
		scale.put("Flash Flood",1);
		scale.put("Flood",1);
		scale.put("Frost/Freeze",1);
		scale.put("Funnel Cloud",1);
		scale.put("Freezing Fog",1);
		scale.put("Hail",1);
		scale.put("Heat",1);
		scale.put("Heavy Rain",1);
		scale.put("Heavy Snow",1);
		scale.put("High Surf",1);
		scale.put("High Wind",1);
		scale.put("Hurricane (Typhoon)",1);
		scale.put("Ice Storm",1);
		scale.put("Lake-Effect Snow",1);
		scale.put("Lakeshore Flood",1);
		scale.put("Lightning",1);
		scale.put("Marine Hail",1);
		scale.put("Marine High Wind",1);
		scale.put("Marine Strong Wind",1);
		scale.put("Marine Thunderstorm Wind",1);
		scale.put("Rip Current",1);
		scale.put("Seiche",1);
		scale.put("Sleet",1);
		scale.put("Storm Surge/Tide",1);
		scale.put("Strong Wind",1);
		scale.put("Thunderstorm Wind",1);
		scale.put("Tornado",1);
		scale.put("Tropical Depression",1);
		scale.put("Tropical Storm",1);
		scale.put("Tsunami",1);
		scale.put("Volcanic Ash",1);
		scale.put("Waterspout",1);
		scale.put("Wildfire",1);
		scale.put("Winter Storm",1);
		scale.put("Winter Weather",1);
		
	  int total=0;
	  for (Text value : values) {
		  //System.out.println(value.toString());
		  total+=scale.get(value.toString());
    }
	  context.write(key, new IntWritable(total));
  }
}