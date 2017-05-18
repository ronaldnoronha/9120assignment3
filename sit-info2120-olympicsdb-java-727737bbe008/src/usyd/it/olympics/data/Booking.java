package usyd.it.olympics.data;

import java.util.Date;
import java.util.HashMap;

public class Booking {
    String vehicle_;                    // Vehicle code
    Date startDay_;                     // Start date
    Date startTime_;                    // Start time
    String memberID_;                   // Member of the booking
    String staffID_;                    // 'bookedby' staff.
    Date whenBooked_;                   // When booked


    public Booking(String vehicle, Date startDay, Date startTime, String memberID, String bookedby, Date whenBooked) {
        vehicle_ = vehicle;
        startDay_ = startDay;
        startTime_ = startTime;
        memberID_ = memberID;
        staffID_ = bookedby;
        whenBooked_ = whenBooked;
    }

    public String getVehicle() {
            return vehicle_;
    }

    public Date getStartTime() {
            return startTime_;
    }

    public Date getStartDay() {
            return startDay_;
    }
    
    public String getMember() {
            return memberID_;
    }

    public String getBookedBy() {
        return staffID_;
    }

    public Date getWhenBooked(){
        return whenBooked_;
    }

	public static String getSummary(HashMap<String, Object> bookingDetails) {
        // TODO: Construct a more useful message based on the contents of bookingDetails
    	String summary = "Display of booking details not yet implemented";
		return summary;
	}



}
