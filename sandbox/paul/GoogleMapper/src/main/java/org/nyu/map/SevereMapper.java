package org.nyu.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;

public class SevereMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final String[] headers = {
		"BEGIN_YEARMONTH",
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
   /*
   private static final String[] headerssample = {
	    "BEGIN_YEARMONTH",
		"BEGIN_DAY",
		"END_YEARMONTH",
		"END_DAY",
		"STATE_FIPS",
		"EVENT_TYPE",
		"CZ_FIPS",
  };
  */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	Configuration conf = context.getConfiguration();
	String FIPS_string = conf.get("FIPS");
	List<String> FIPS = Arrays.asList(FIPS_string.split(" "));
	
	if(Character.isAlphabetic(line.charAt(0))){
		return;  //ignore header
	}
	//comma separated line
	String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
	
	//clean column fields
	HashMap<String, String> row = new HashMap<String, String>();
	int count=0;
    for(String token : tokens){
    	//remove quoted fields
    	if (token.startsWith("\"") && token.endsWith("\"") || token.startsWith("\'") && token.endsWith("\'")){
    		token = token.substring(1,token.length() -1);
    	}
    	//county fips should have 3 digits
    	if(headers[count]=="CZ_FIPS"){
    		if(token.length()==2){
    			token = String.format("%03d", Integer.parseInt(token));
    		}
    	}
    	//remove spaces in event type
    	if(headers[count]=="EVENT_TYPE"){
    		token = token.replace(" ", "");
    	}
	    row.put(headers[count],token);
	    count++;
    }
    
    //some storms span multiple days
	Calendar stormstart = Calendar.getInstance();
	stormstart.set(Calendar.YEAR, Integer.parseInt(row.get("BEGIN_YEARMONTH").substring(0,4)));
	stormstart.set(Calendar.DAY_OF_MONTH, Integer.parseInt(row.get("BEGIN_DAY")));
	stormstart.set(Calendar.MONTH, Integer.parseInt(row.get("BEGIN_YEARMONTH").substring(4)));
	Calendar stormend = Calendar.getInstance();
	stormend.set(Calendar.YEAR, Integer.parseInt(row.get("BEGIN_YEARMONTH").substring(0,4)));
	stormend.set(Calendar.DAY_OF_MONTH, Integer.parseInt(row.get("END_DAY")));
	stormend.set(Calendar.MONTH, Integer.parseInt(row.get("END_YEARMONTH").substring(4)));

	while(!stormstart.after(stormend)){
		String statecounty = row.get("STATE_FIPS")+row.get("CZ_FIPS");
		if(FIPS.contains(statecounty)){  //conditional for performance consideration only 
			
			//determine all 'vacation dates' that will be affected by the storm 
			Calendar firstDay = Calendar.getInstance();
			firstDay.set(Calendar.YEAR, 2010); //do not pick a leap year
			firstDay.set(Calendar.DAY_OF_YEAR,stormstart.get(Calendar.DAY_OF_YEAR));
			firstDay.add(Calendar.DATE, (FIPS.size()-1)*-1);
			Calendar endDay = Calendar.getInstance();
			endDay.set(Calendar.YEAR, 2010); //do not pick a leap year
			endDay.set(Calendar.DAY_OF_YEAR,stormstart.get(Calendar.DAY_OF_YEAR));
			while(!firstDay.after(endDay)){				
				List<String> vacationdays = new ArrayList<String>();
				for(int i=0;i<FIPS.size();i++){
					Calendar currentDay = Calendar.getInstance();
					currentDay.set(Calendar.YEAR, 2010); //do not pick a leap year
					currentDay.set(Calendar.DAY_OF_YEAR,firstDay.get(Calendar.DAY_OF_YEAR));
					currentDay.add(Calendar.DATE, i);
					vacationdays.add(String.format("%03d",currentDay.get(Calendar.DAY_OF_YEAR)));
				}
				firstDay.add(Calendar.DATE, 1);
				
				//only emit 'vacation dates' when the storm date=the day we are there 
				if(vacationdays.indexOf(String.format("%03d", stormstart.get(Calendar.DAY_OF_YEAR)))==FIPS.indexOf(statecounty)){
					context.write(new Text(Joiner.on(" ").join(vacationdays)), 
							new Text(String.format("%03d",stormstart.get(Calendar.DAY_OF_YEAR))+"_"+stormstart.get(Calendar.YEAR)+"_"+statecounty+"_"+row.get("EVENT_TYPE")));
				}
				
			}
		}
	    stormstart.add(Calendar.DATE, 1);
	}
  }
}
