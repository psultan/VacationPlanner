
package nyu.traffic;

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

//import com.google.common.base.Joiner;

public class TrafficFatalityMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final String[] headers = {
		"DAY,N,16,0",
		"MONTH,N,16,0",
		"YEAR,N,16,0",
		"LATITUDE,N,16,8",
		"LONGITUDE,N,16,8",
		"STATE,N,16,0",
		"COUNTY,N,16,0",
		"HARM_EV,N,16,0",
		"FATALS,N,16,0",
		"DRUNK_DR,N,16,0",
  };
   
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	Configuration conf = context.getConfiguration();
	String FIPS_string = conf.get("FIPS");
	List<String> FIPS = Arrays.asList(FIPS_string);
	
	if(Character.isAlphabetic(line.charAt(0))){
		return;  //ignore header
	}
	if (line.contains("DAY")) {
		return;
	}
	
	System.out.println(line);
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
    	if(headers[count]=="COUNTY,N,16,0"){
    		if(token.length()!=3 && !"".equals(token)){
    			token = String.format("%03d", Integer.parseInt(token));
    		}
    	}
    	
    	//System.out.println("TOKEN: " + token);
    	//state fips should have 2 digits
    	if(headers[count]=="STATE,N,16,0"){
    		if(token.length()!=2 && !"".equals(token)){
    			token = String.format("%02d", Integer.parseInt(token));
    		}
    	}
    	/*remove spaces in event type
    	if(headers[count]=="EVENT_TYPE"){
    		token = token.replace(" ", "");
    	}*/
	    row.put(headers[count],token);
	    count++;
    }
    
    //some storms span multiple days
    //System.out.println(row.get("DAY,N,16,0")+":"+row.get("MONTH,N,16,0")+":"+row.get("YEAR,N,16,0")+"_"+line);
	Calendar stormstart = Calendar.getInstance();
	//System.out.println("ROW:" + row.get("YEAR,N,16,0"));	
	stormstart.set(Calendar.YEAR, Integer.parseInt(row.get("YEAR,N,16,0")));
	stormstart.set(Calendar.DAY_OF_MONTH, Integer.parseInt(row.get("DAY,N,16,0")));
	stormstart.set(Calendar.MONTH, Integer.parseInt(row.get("MONTH,N,16,0")));
	//Calendar stormend = Calendar.getInstance();
	/*stormend.set(Calendar.YEAR, Integer.parseInt(row.get("BEGIN_YEARMONTH").substring(0,4)));
	stormend.set(Calendar.DAY_OF_MONTH, Integer.parseInt(row.get("END_DAY")));
	stormend.set(Calendar.MONTH, Integer.parseInt(row.get("END_YEARMONTH").substring(4))-1);*/

	//loop over all days of the storm
	//while(!stormstart.after(stormend)){
		String statecounty = row.get("STATE,N,16,0")+row.get("COUNTY,N,16,0");
		//System.out.println(statecounty);
		//System.out.println(FIPS);
		//if(FIPS.contains(statecounty)){  //conditional for performance consideration only 
			//determine all 'vacation dates' that will be affected by the storm 
			Calendar firstDay = Calendar.getInstance();
			firstDay.set(Calendar.YEAR, stormstart.get(Calendar.YEAR)); 
			firstDay.set(Calendar.DAY_OF_YEAR,stormstart.get(Calendar.DAY_OF_YEAR));
			firstDay.add(Calendar.DATE, (FIPS.size()-1)*-1);
			//System.out.println(firstDay);
			//Calendar endDay = Calendar.getInstance();
			//endDay.set(Calendar.YEAR, stormstart.get(Calendar.YEAR));
			//endDay.set(Calendar.DAY_OF_YEAR,stormstart.get(Calendar.DAY_OF_YEAR));
			//while(!firstDay.after(endDay)){				
				List<String> vacationdays = new ArrayList<String>();
				for(int i=0;i<FIPS.size();i++){
					Calendar currentDay = Calendar.getInstance();
					currentDay.set(Calendar.YEAR, 2010); //do not pick a leap year
					currentDay.set(Calendar.DAY_OF_YEAR,firstDay.get(Calendar.DAY_OF_YEAR));
					currentDay.add(Calendar.DATE, i);
					vacationdays.add(String.format("%03d",currentDay.get(Calendar.DAY_OF_YEAR)));
					System.out.println(vacationdays);
				}
				
				
				//only emit 'vacation dates' when the storm date=the day we are there 
				if(vacationdays.indexOf(String.format("%03d", stormstart.get(Calendar.DAY_OF_YEAR)))==FIPS.indexOf(statecounty)){
					//System.out.println(Joiner.on(" ").join(vacationdays)+", "+String.format("%03d",stormstart.get(Calendar.DAY_OF_YEAR))+"_"+stormstart.get(Calendar.YEAR)+"_"+statecounty+"_"+row.get("EVENT_TYPE"));
					//001 002 003 004, 001_2015_12305_Hail
					//vacation days, stormday_stormyear_statecounty_stormtype
					//System.out.println("WRITING MAP OUTPUTS");
					context.write(new Text(Joiner.on(" ").join(vacationdays)), 
							new Text(String.format("%03d",firstDay.get(Calendar.DAY_OF_YEAR))+"_"+firstDay.get(Calendar.YEAR)+"_"+statecounty+"_"+row.get("HARM_EV,N,16,0")));
				}
				
				firstDay.add(Calendar.DATE, 1);
			}
		}
  
	   // stormstart.add(Calendar.DAT, 1);
  //}
