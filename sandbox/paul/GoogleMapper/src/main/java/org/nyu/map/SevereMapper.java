package org.nyu.map;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SevereMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	if(Character.isAlphabetic(line.charAt(0))){
		return;  //ignore header
	}
	String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
	
	HashMap<String, String> row = new HashMap<String, String>();
	String[] headers = {"BEGIN_YEARMONTH",
						"BEGIN_DAY",
						"BEGIN_TIME",
						"END_YEARMONTH",
						"END_DAY",
						"END_TIME",
						"EPISODE_ID",
						"EVENT_ID",
						"STATE",
						"STATE_FIPS",
						"YEAR",
						"MONTH_NAME",
						"EVENT_TYPE",
						"CZ_TYPE",
						"CZ_FIPS",
						"CZ_NAME",
						"WFO",
						"BEGIN_DATE_TIME",
						"CZ_TIMEZONE",
						"END_DATE_TIME",
						"INJURIES_DIRECT",
						"INJURIES_INDIRECT",
						"DEATHS_DIRECT",
						"DEATHS_INDIRECT",
						"DAMAGE_PROPERTY",
						"DAMAGE_CROPS",
						"SOURCE",
						"MAGNITUDE",
						"MAGNITUDE_TYPE",
						"FLOOD_CAUSE",
						"CATEGORY",
						"TOR_F_SCALE",
						"TOR_LENGTH",
						"TOR_WIDTH",
						"TOR_OTHER_WFO",
						"TOR_OTHER_CZ_STATE",
						"TOR_OTHER_CZ_FIPS",
						"TOR_OTHER_CZ_NAME",
						"BEGIN_RANGE",
						"BEGIN_AZIMUTH",
						"BEGIN_LOCATION",
						"END_RANGE",
						"END_AZIMUTH",
						"END_LOCATION",
						"BEGIN_LAT",
						"BEGIN_LON",
						"END_LAT",
						"END_LON",
						"EPISODE_NARRATIVE",
						"EVENT_NARRATIVE",
						"DATA_SOURCE"
	};

	
	int count=0;
    for(String token : tokens){
	    row.put(headers[count],token);
	    count++;
    }
    context.write(new Text("BEGIN_YEARMONTH"),new Text(row.get("BEGIN_YEARMONTH")));
  }
}
