package usyd.it.olympics;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import javax.swing.SwingUtilities;

import usyd.it.olympics.data.Booking;
import usyd.it.olympics.data.Place;
import usyd.it.olympics.gui.GuiFrontEnd;

public class OlympicsDBClient {
    // All GUI stuff is performed by this object

    private final GuiFrontEnd gui;
    // All database operations (logging in, running queries) are performed by this object
    private DatabaseBackend db;
	private String memberID; // Member ID
	private String memberType;

    OlympicsDBClient(String config) {
        // Make sure the DB backend works
        try {
            db = new DatabaseBackend(new FileInputStream(config)); // Note, doesn't connect to DB
        } catch (Exception e) {
            // Can't do much so die noisily.
            e.printStackTrace();
            System.exit(1);
        }
        gui = new GuiFrontEnd(this);
        setMessage("Welcome to Olympics DB Client.");
    }

    private void setMessage(String msg) {
        gui.setStatus(msg);
    }

    //
    // Client user login/logout
    //
    public void login(String memUser, char [] memPass) {
        setMessage("Connecting to DB.");
        try {
        	memberID = null;
        	HashMap<String, Object> basicDetails = db.checkLogin(memUser, memPass);
            if(basicDetails!=null) {
            	memberID = memUser;
            	memberType = (String) basicDetails.get("member_type");
            	setMessage("Verified login, Fetching member details");
            	HashMap<String, Object> fullDetails = db.memberDetails(memberID, memberType);
            	gui.getMainMenuScreen().showMemberDetails(fullDetails);
            	gui.showMainMenuScreen();
            	setMessage("Login successful.");
            } else {
            	setMessage("Login details incorrect.");
            }
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
        }
    }

    public void logout() {
        setMessage("Logging out");
        memberID = null;
        gui.showLoginScreen();
        setMessage("Logged out");
    }

    //
    // Member info 
    //
    public void showMemberDetails() {
        setMessage("Fetching member details.");
        try {
            HashMap<String, Object> member = db.memberDetails(memberID, memberType);
            gui.getMainMenuScreen().showMemberDetails(member);
            gui.showMainMenuScreen();
            setMessage("Details fetched.");
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
        }
    }


    
    /**
     * Create a new client GUI, running on its own event thread
     *
     * @param args Ignored
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {

            @Override
            public void run() {
                new OlympicsDBClient("olympicsdb.properties");
            }
        });
    }

    //
    // Bay Finder
    //
    public void showMatchingBays(String address) {
        setMessage("Fetching bay availabilities.");
        try {
            ArrayList<HashMap<String, Object>> bays = db.getMatchingBays(address);
            gui.getBayFinderScreen().showTuples(bays);
            setMessage("All bays fetched.");
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
            gui.getBayFinderScreen().showTuples(new ArrayList<HashMap<String, Object>>());
        }
        gui.showBayFinderScreen();
    }
    
    public void showBayAvailability() {
        setMessage("Fetching bay availabilities.");
        gui.showBayFinderScreen();
    }
    
    public void getBayDetails(int bay) {
        setMessage("Retrieving details");
        try {
        	HashMap<String, Object> details = db.getBayDetails(bay);
            gui.getBayDetailsScreen().showBayDetails(details);
            gui.showBayDetailsScreen();
            setMessage("Details retrieved");
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
        }
    }

    public void makeBooking(String forMember, String vehicle, Date departs) {
        setMessage("Submitting booking");
        try {
        	HashMap<String,Object> bookingDetails = db.makeBooking(memberID, 
        			forMember, departs);
        	if(bookingDetails==null) {
                setMessage("Could not make booking");
        	} else {
                gui.getReportScreen().show(Booking.getSummary(bookingDetails));
                gui.showReportScreen();
                setMessage("Submission complete");
        	}
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
        }
    }
    
    public void showFavouriteBay() {
        setMessage("Retrieving bay details");
        try {
        	HashMap<String,Object> bay = db.getFavouriteBay();
            if (bay == null)
                setMessage("No bay details found");
            else {    
                gui.getBayDetailsScreen().showBayDetails(bay);
                gui.showBayDetailsScreen();
                setMessage("Bay details retrieved");
            }
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
        }    
    }

    public void showHistory() {
        setMessage("Fetching booking history.");
        try {
            ArrayList<HashMap<String,Object>> bookings = db.allBookings();
            gui.getHistoryScreen().showBookings(bookings);
            gui.showHistoryScreen();
            setMessage("All bookings fetched.");
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
        }    
    }

    public void startBooking(int bayId) {
        setMessage("Fetching details to start booking.");
        //HashMap<String, Object> place = db.getMemberHome(memberID);
		//Integer placeid = Place.getPlaceId(place);
		gui.getBookingsCreationScreen().startBooking();
		gui.showBookingsCreationScreen();
		setMessage("Details fetched."); 
    }

    public void showBookingDetails(int journeyId) {
       setMessage("Getting booking details");
        try {
        	HashMap<String,Object> bookingDetails = db.getBookingDetails(memberID, journeyId);
            gui.getReportScreen().show(Booking.getSummary(bookingDetails));
            gui.showReportScreen();
            setMessage("Details fetched.");
        } catch (OlympicsDBException e) {
            setMessage(e.getMessage());
        }
    }


}
