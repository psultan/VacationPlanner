package org.nyu.map;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SevereMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	Configuration conf = context.getConfiguration();
	String FIPS = conf.get("FIPS");
	System.out.println(FIPS);
	
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
    	if (token.startsWith("\"") && token.endsWith("\"") || token.startsWith("\'") && token.endsWith("\'")){
    		token = token.substring(1,token.length() -1);
    	}
	    row.put(headers[count],token);
	    count++;
    }
    
	Calendar start = Calendar.getInstance();
	start.set(Calendar.DAY_OF_MONTH, Integer.parseInt(row.get("BEGIN_DAY")));
	start.set(Calendar.MONTH, Integer.parseInt(row.get("BEGIN_YEARMONTH").substring(4)));
	start.set(Calendar.YEAR, Integer.parseInt(row.get("BEGIN_YEARMONTH").substring(0,4)));
	Calendar end = Calendar.getInstance();
	end.set(Calendar.DAY_OF_MONTH, Integer.parseInt(row.get("END_DAY")));
	end.set(Calendar.MONTH, Integer.parseInt(row.get("END_YEARMONTH").substring(4)));
	end.set(Calendar.YEAR, Integer.parseInt(row.get("END_YEARMONTH").substring(0,4)));

	while(!start.after(end)){
		//System.out.println(row.get("STATE_FIPS")+row.get("CZ_FIPS")+"_"+start.get(Calendar.DAY_OF_YEAR)+" "+row.get("EVENT_TYPE"));
	    context.write(new Text(row.get("STATE_FIPS")+row.get("CZ_FIPS")+"_"+start.get(Calendar.DAY_OF_YEAR)), new Text(row.get("EVENT_TYPE")));
	    start.add(Calendar.DATE, 1);
	}
  }
}
