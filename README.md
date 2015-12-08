# VacationPlanner

We will look to answer the question, "When would be the best time of year to take a road trip?"  Specifically we will be taking a look at historic data for temperature, natural disasters, precipitation and traffic accidents to find the optimal time range for such a trip.  As a New Yorker the naive answer would be some time in the middle of May or September when the temperature is in the mid 70s to 80s.  However how does that temperature compare to Utah's at the same time? Or does that time coincide with tornado season in the west?  We will look to find the specific dates that would be the best, empirically, given a starting location, destination and duration. For example, "When would be the best time to go from New York to San Francisco in 30 days."  In addition to analyzing the weather data we will first need to determine a basic course plot to determine what locations we will be at based on the input.


  - Paul Sultan ps1298
  - Ankur Chaudhary ac5435
  - Vipul Daundkar vtd213


Input: Start Location, End Location, Duration in days, Ideal Temperature (Ny, California, 7, 75)

Phase 1: Process route:

    - latlng: [[41.243900000000004, -81.08354000000001], [41.375780000000006, -88.656], [41.19908, -96.09541], [40.44715, -103.31071000000001], [38.944370000000006, -109.64305], [36.366440000000004, -114.90073000000001], [36.7782392, -119.4179254]]
    - state_countyFIPS: [39133, 17099, 31055, 08075, 49019, 32003, 06019]
