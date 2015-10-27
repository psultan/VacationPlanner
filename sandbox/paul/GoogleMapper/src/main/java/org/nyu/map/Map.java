package org.nyu.map;

import java.util.Calendar;
import java.util.List;

import com.google.maps.DirectionsApi;
import com.google.maps.internal.PolylineEncoding;
import com.google.maps.model.DirectionsRoute;
import com.google.maps.model.DirectionsStep;
import com.google.maps.model.LatLng;
import com.google.maps.GeoApiContext;

public class Map {
	public static void main(String[] args) throws Exception {
		GeoApiContext context = new GeoApiContext().setApiKey("AIzaSyCjb8Xx90GcIB9x2nJYuA2MLphWQ4lMDcU");

		int desiredDays = 10;
		int desiredSeconds = desiredDays*24*60*60;
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_YEAR, 120);
		
		
        DirectionsRoute[] routes = DirectionsApi.getDirections(context, "NY", "San Francisco").await();
        long inSeconds = routes[0].legs[0].duration.inSeconds;
        long inMeters = routes[0].legs[0].distance.inMeters;
    	System.out.println("Steps:"+routes[0].legs[0].steps.length);
    	System.out.println("Seconds:"+inSeconds);
    	System.out.println("Meters:"+inMeters);

    	long metersperday = inMeters/desiredDays;
    	System.out.println("meters per day:"+metersperday);
    	long totalMeters=0;
    	long totalSeconds=0;
    	int index = 0;
    	for (DirectionsStep step : routes[0].legs[0].steps){
        	totalMeters+=step.distance.inMeters;
        	totalSeconds+=step.duration.inSeconds;
        	System.out.println(index +" "+ step.distance.inMeters+"m "+ step.duration.inSeconds+"s");
        	index+=1;
    	}
    	
    	List<com.google.maps.model.LatLng> path = PolylineEncoding.decode(routes[0].legs[0].steps[0].polyline.getEncodedPath());
    	
    	long distance=0;
    	LatLng firstpoint=path.get(0);
    	for (LatLng point : path){
    		double newDistance = haversine(firstpoint.lat, firstpoint.lng, point.lat, point.lng);
    		distance+=newDistance;
    		System.out.println("distance:"+distance+"m ("+firstpoint.lat+","+firstpoint.lng+") ("+point.lat+","+point.lng+")" );
    		
    		firstpoint=point;
    	}
    	
    	System.out.println("TotalSeconds:"+totalSeconds+"s");
    	System.out.println("TotalMeters:"+totalMeters+"m");
	}
	
    public static final double R = 6372800; //earth radius in meters
    public static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);
 
        double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }
	
}

