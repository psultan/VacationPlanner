package nyu.vacation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;

public class SevereReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
		HashMap<String, int[]> scale = new HashMap<String, int[]>();
		scale.put("HighSurf"                 , new int[]{0,0} );
		scale.put("AstronomicalLowTide"      , new int[]{0,0} );
		scale.put("RipCurrent"               , new int[]{0,0} );
		scale.put("Seiche"                   , new int[]{0,0} );
		scale.put("HighWind"                 , new int[]{1,0} );
		scale.put("MarineHighWind"           , new int[]{1,0} );
		scale.put("StrongWind"               , new int[]{2,0} );
		scale.put("MarineThunderstormWind"   , new int[]{2,0} );
		scale.put("WinterWeather"            , new int[]{2,0} );
		scale.put("Drought"                  , new int[]{2,0} );
		scale.put("MarineStrongWind"         , new int[]{2,0} );
		scale.put("DenseSmoke"               , new int[]{3,0} );
		scale.put("DenseFog"                 , new int[]{3,0} );
		scale.put("Sleet"                    , new int[]{3,0} );
		scale.put("Frost/Freeze"             , new int[]{3,0} );
		scale.put("StormSurge/Tide"          , new int[]{3,0} );
		scale.put("FreezingFog"              , new int[]{3,0} );
		scale.put("DebrisFlow"               , new int[]{3,0} );
		scale.put("Heat"                     , new int[]{3,0} );
		scale.put("HeavyRain"                , new int[]{3,0} );
		scale.put("HeavySnow"                , new int[]{3,0} );
		scale.put("MarineHail"               , new int[]{4,0} );
		scale.put("Hail"                     , new int[]{4,0} );
		scale.put("Cold/WindChill"           , new int[]{4,0} );
		scale.put("ThunderstormWind"         , new int[]{4,0} );
		scale.put("ExtremeCold/WindChill"    , new int[]{4,0} );
		scale.put("ExcessiveHeat"            , new int[]{4,0} );
		scale.put("LakeshoreFlood"           , new int[]{5,0} );
		scale.put("DustStorm"                , new int[]{5,0} );
		scale.put("FunnelCloud"              , new int[]{5,0} );
		scale.put("Lake-EffectSnow"          , new int[]{5,0} );
		scale.put("WinterStorm"              , new int[]{5,0} );
		scale.put("DustDevil"                , new int[]{6,0} );
		scale.put("Lightning"                , new int[]{6,0} );
		scale.put("IceStorm"                 , new int[]{6,0} );
		scale.put("Waterspout"               , new int[]{6,0} );
		scale.put("Flood"                    , new int[]{7,0} );
		scale.put("TropicalDepression"       , new int[]{7,0} );
		scale.put("Wildfire"                 , new int[]{7,0} );
		scale.put("TropicalStorm"            , new int[]{7,0} );
		scale.put("Tornado"                  , new int[]{8,0} );
		scale.put("Blizzard"                 , new int[]{8,0} );
		scale.put("FlashFlood"               , new int[]{8,0} );
		scale.put("Hurricane(Typhoon)"       , new int[]{9,0} );
		scale.put("CoastalFlood"             , new int[]{9,0} );
		scale.put("VolcanicAsh"              , new int[]{9,0} );
		scale.put("Avalanche"                , new int[]{10,0} );
		scale.put("Tsunami"                  , new int[]{10,0} );

	ArrayList<Text> cache = new ArrayList<Text>();
	for(Text value: values){
		System.out.println(value);
		cache.add(new Text(value));
		String[] stormtype = value.toString().split("_");
		int[] scalevalues = scale.get(stormtype[stormtype.length-1]);
		System.out.println(scalevalues[0]+" "+scalevalues[1]);
		scalevalues[1]++;
	}
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
	context.write(new Text(startday),new Text(StringUtils.leftPad(totals,totals.length(),'0')+"\t"+Joiner.on(" ").join(tallies)));
	//output with total and tallies and dates
	//context.write(new Text(startday),new Text(StringUtils.leftPad(totals,totals.length(),'0')+" "+Joiner.on(" ").join(tallies)+" "+Joiner.on(" ").join(cache)));
  }
}