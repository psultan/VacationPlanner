//get latlng, fips for start location, end location, and duration
//author: paul

package nyu.vacation;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Arrays;

import org.json.JSONObject;

import com.google.maps.DirectionsApi;
import com.google.maps.GeocodingApi;
import com.google.maps.model.DirectionsRoute;
import com.google.maps.model.DirectionsStep;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.LatLng;
import com.google.maps.GeoApiContext;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Map {
	public double[][] getLatLng(String startLocation, String endLocation, String days) throws Exception {
		int desiredDays = Integer.parseInt(days);
		GeoApiContext context = new GeoApiContext().setApiKey(System.getenv("MAPKEY"));

        DirectionsRoute[] routes = DirectionsApi.getDirections(context, startLocation, endLocation).await();
        long inSeconds = routes[0].legs[0].duration.inSeconds;
        long inMeters = routes[0].legs[0].distance.inMeters;
    	System.out.println("Steps:"+routes[0].legs[0].steps.length);
    	System.out.println("Seconds:"+inSeconds);
    	System.out.println("Meters:"+inMeters);

    	long metersPerDay = inMeters/desiredDays;
    	System.out.println("Meters/Day:"+metersPerDay);
    	
		long dailyMeter=0;
		int day=0;
		double[][] vacation = new double[desiredDays][2];
    	for(int i=0;i<routes[0].legs[0].steps.length-1;i++){
    		DirectionsStep step = routes[0].legs[0].steps[i];
    		if(dailyMeter+step.distance.inMeters<metersPerDay){
    			//next step is less than daily max
    			dailyMeter+=step.distance.inMeters;
    		}
    		else if(dailyMeter+step.distance.inMeters>metersPerDay){
    			//next step is greater than daily max
    			//use subdivided polyline for shorter steps
    			List<LatLng> path = step.polyline.decodePath();
    			long polyTotal=step.distance.inMeters;
    			long polyTraveled=0;
    			int currentStep=0;
    			int daysInPoly=0;
    			while(polyTraveled==0 || polyTraveled+metersPerDay<polyTotal){ 
    				//always run atleast once
    				//loop over if the next day is all in polyline
	    			LatLng firstpoint=path.get(currentStep);
	    			while(currentStep+1<path.size()){
	    				currentStep+=1;
	    				LatLng point=path.get(currentStep);
	    				double minorStep = haversine(firstpoint.lat, firstpoint.lng, point.lat, point.lng);
	    				polyTraveled+=minorStep;
	    				dailyMeter+=minorStep;
	    				//System.out.format("---%d-%d-step%d/%d, distance:%10.3fm %8d/%-8d %8d/%-8d (%6.3f, %6.3f) (%6.3f, %6.3f) %n",
	    				//					  day,daysInPoly,currentStep,path.size(),minorStep,    polyTraveled,polyTotal,    dailyMeter,metersPerDay*(daysInPoly+1),      firstpoint.lat,firstpoint.lng,point.lat,point.lng);
	    				firstpoint=point;
	    				
	    				if(dailyMeter>=metersPerDay){
	    					vacation[day][0]=firstpoint.lat;
	    					vacation[day][1]=firstpoint.lng;
	    					day+=1;
	    					dailyMeter=0;
	    					daysInPoly+=1;
	    					break;
	    				}
	    			}
    			}
				dailyMeter = polyTotal - polyTraveled;
    		}
    	}
    	vacation[desiredDays-1][0]=routes[0].legs[0].endLocation.lat;
		vacation[desiredDays-1][1]=routes[0].legs[0].endLocation.lng;
    	
		System.out.println(Arrays.deepToString(vacation));
    	System.out.println(routes[0].overviewPolyline.getEncodedPath().replace("\\","\\\\")); //the \ should not be treated as an escape character (this may not work with \\u)
    	//in case encodedPath fails
    	//for (LatLng point : routes[0].overviewPolyline.decodePath()){
    	//	System.out.format("[%f,%f],",point.lat, point.lng);
    	//}
		return vacation;
	}
		
	public List<String> getFIPS (double[][] latlngs){
    	//get fips from latlng
		List<String> FIPS = new ArrayList<String>();
    	for(double[] latlng : latlngs){
	    	Client client = Client.create();
			WebResource webResource = client.resource("http://data.fcc.gov/api/block/find?format=json&latitude="+latlng[0]+"&longitude="+latlng[1]+"&showall=true");
			ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
			if (response.getStatus() != 200) {
			   throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}
			response.bufferEntity();
			String output = response.getEntity(String.class);
			JSONObject obj = new JSONObject(output);
			FIPS.add(obj.getJSONObject("County").getString("FIPS"));
    	}
		return FIPS;
	}
	
    public static final double R = 6372800; //earth radius in meters
    public static double haversine(double lat1, double lon1, double lat2, double lon2) {
    	//get distance between two points 
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);
 
        double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }
	
}

