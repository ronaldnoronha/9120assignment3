package usyd.it.olympics.data;

import java.util.Date;
import java.util.HashMap;

/**
 * Created by INFO2120
 */
public class OlympicJourney implements BasicEntity {
    String vehicle_;
    Date startDate_;
    String toPlace_;
    String fromPlace_;
    int nbooked_;
    int vehicleCapacity_;

    public OlympicJourney(String vehicle, Date startDate, String toPlace, String fromPlace, int nbooked, int vehicleCapacity){
        vehicle_ = vehicle;
        startDate_ = startDate;
        toPlace_ = toPlace;
        fromPlace_ = fromPlace;
        nbooked_ = nbooked;
        vehicleCapacity_ = vehicleCapacity;
    }

    @Override
    public HashMap<String, Object> asHashMap() {
        HashMap<String, Object> journey = new HashMap<>();
        journey.put("vehicle" , vehicle_);
        journey.put("startDate" , startDate_);
        journey.put("to", toPlace_);
        journey.put("from", fromPlace_);
        journey.put("nbooked" , nbooked_);
        journey.put("vehicleCapacity" , vehicleCapacity_);

        return journey;
    }
}
