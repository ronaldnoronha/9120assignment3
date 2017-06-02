package usyd.it.olympics;
import java.sql.*;
import java.text.SimpleDateFormat;
/**
 * Database back-end class for simple gui.
 * 
 * The DatabaseBackend class defined in this file holds all the methods to 
 * communicate with the database and pass the results back to the GUI.
 *
 *
 * Make sure you update the dbname variable to your own database name. You
 * can run this class on its own for testing without requiring the GUI.
 */
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

/**
 * Database interfacing backend for client. This class uses JDBC to connect to
 * the database, and provides methods to obtain query data.
 * 
 * Most methods return database information in the form of HashMaps (sets of 
 * key-value pairs), or ArrayLists of HashMaps for multiple results.
 *
 * @author Bryn Jeffries {@literal <bryn.jeffries@sydney.edu.au>}
 */
public class DatabaseBackend {

	///////////////////////////////
	/// DB Connection details
	///////////////////////////////
	private final String dbUser;
	private final String dbPass;
	private final String connstring;


	///////////////////////////////
	/// Student Defined Functions
	///////////////////////////////

	/////  Login and Member  //////

	/**
	 * Validate memberID details
	 * 
	 * Implements Core Functionality (a)
	 *
	 * @return true if username is for a valid memberID and password is correct
	 * @throws OlympicsDBException 
	 * @throws SQLException
	 */
	public HashMap<String,Object> checkLogin(String member, char[] password) throws OlympicsDBException  {
		HashMap<String,Object> details = null;
		try {
			Connection conn = getConnection();
			String query = "select * from Member where member_id = ? and pass_word = ?";
			PreparedStatement stmt = null;
			String pass_word = new String(password);
//			for (int i= 0;i<password.length;i++){
//				pass_word+=password[i];
//			}
			//System.out.println(member+"  "+pass_word);
			stmt = conn.prepareStatement(query);
			stmt.setString(1,member);
			stmt.setString(2,pass_word);
			//stmt.setString(1,"A000028091");
			//stmt.setString(2,"diamond");

			ResultSet rs = stmt.executeQuery();


			if (rs.next()) {

				details = new HashMap<String,Object>();

				details.put("member_id", member);
				details.put("title", rs.getString("title"));
				details.put("first_name", rs.getString("given_names"));
				details.put("family_name", rs.getString("family_name"));

				// write further queries
				details.put("country_name", rs.getString("country_code"));
				details.put("residence", rs.getString("accommodation"));
				details.put("member_type", "blank");
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Error checking login details", e);
		}
		return details;
	}

	/**
	 * Obtain details for the current memberID
	 * @param memberID 
	 * @param member_type 
	 *
	 *
	 * @return text to be displayed in the home screen
	 * @throws OlympicsDBException
	 */
	public HashMap<String, Object> getMemberDetails(String memberID) throws OlympicsDBException {

		String query = "select * from Member where member_id = ?";
		PreparedStatement stmt = null;
		
		try {
			Connection conn = getConnection();
			stmt = conn.prepareStatement(query);
			stmt.setString(1,memberID);

			ResultSet rs = stmt.executeQuery();

			// FIXME: REPLACE FOLLOWING LINES WITH REAL OPERATION
			HashMap<String, Object> details = new HashMap<String, Object>();
			//details.put(arg0, arg1)= "Hello Mr Joe Bloggs";
			String accommodation = null;
			String country_code = null;
			while (rs.next()){
				details.put("member_id", memberID);
				details.put("title", rs.getString("title"));			
				details.put("first_name",rs.getString("given_names"));
				details.put("family_name",rs.getString("family_name"));
				accommodation = rs.getString("accommodation");
				country_code = rs.getString("country_code");

			}
			rs.close();
			stmt.close();

			String[] member_types = {"athlete", "official", "staff"};

			
			for (int i = 0;i<member_types.length;i++){	
				
				query = "select member_id from "+ member_types[i]+" where member_id = '" + memberID+ "'";				
				stmt = conn.prepareStatement(query);				
				rs = stmt.executeQuery();
				if(rs.next()){
					if (details.get("member_type")==null){
						
						details.put("member_type",member_types[i]);
					}else{
						details.put("member_type",details.get("member_type")+", "+member_types[i]);						
					}					
				}				
				rs.close();
				stmt.close();
			}
			

			query = "select country_name from country where country_code = ?";			
			stmt = conn.prepareStatement(query);
			stmt.setString(1,country_code);
			rs = stmt.executeQuery();			
			while (rs.next()){				
				details.put("country_name",rs.getString("country_name"));
			}
			rs.close();
			stmt.close();

			query = "select place_name from place where place_id = ?";
			stmt = conn.prepareStatement(query);
			stmt.setString(1,accommodation);
			rs = stmt.executeQuery();			
			while (rs.next()){				
				details.put("residence",rs.getString("place_name"));
			}
			rs.close();
			stmt.close();
			
			
			query = "select count(*) as c from Booking where booked_by = '"+memberID+"'";
			stmt = conn.prepareStatement(query);			
			//stmt.setString(1,memberID);
			rs = stmt.executeQuery();
			int nr = 0;			
			while (rs.next()){
				nr++;
				//System.out.println(rs.getString("c"));			
				details.put("num_bookings",rs.getString("c"));
			}
			//details.put("num_bookings",Integer.valueOf(nr));
			rs.close();
			stmt.close();
			
			query = "select medal from participates where athlete_id = ?";			
			stmt = conn.prepareStatement(query);
			stmt.setString(1,memberID);
			rs = stmt.executeQuery();
			
			int bronze = 0;
			int silver = 0;
			int gold = 0;
			while (rs.next()){												
				if (rs.getString("medal").equals("B")){
					bronze+=1;
				}
				else if (rs.getString("medal").equals("S")){
					silver+=1;
				}
				else if (rs.getString("medal").equals("G")){
					gold+=1;
				}
				
			}
			
			
			
			rs.close();
			stmt.close();
			conn.close();
			
			details.put("num_gold", Integer.valueOf(gold));
			details.put("num_silver", Integer.valueOf(silver));
			details.put("num_bronze", Integer.valueOf(bronze));

			return details;
		} catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Error checking login details", e);
		}
	}


	//////////  Events  //////////

	/**
	 * Get all of the events listed in the olympics for a given sport
	 *
	 * @param sportname the ID of the sport we are filtering by
	 * @return List of the events for that sport
	 * @throws OlympicsDBException
	 */
	ArrayList<HashMap<String, Object>> getEventsOfSport(Integer sportname) throws OlympicsDBException {
		// FIXME: Replace the following with REAL OPERATIONS!

		ArrayList<HashMap<String, Object>> events = new ArrayList<>();

		HashMap<String,Object> event1 = new HashMap<String,Object>();
		event1.put("event_id", Integer.valueOf(123));
		event1.put("sport_id", Integer.valueOf(3));
		event1.put("event_name", "Women's 5km Egg & Spoon");
		event1.put("event_gender", "W");
		event1.put("sport_venue", "ANZ Stadium");
		event1.put("event_start", new Date());
		events.add(event1);

		HashMap<String,Object> event2 = new HashMap<String,Object>();
		event2.put("event_id", Integer.valueOf(123));
		event2.put("sport_id", Integer.valueOf(3));
		event2.put("event_name", "Men's 40km Cross-country Hopping");
		event2.put("event_gender", "M");
		event2.put("sport_venue", "Bennelong Point");
		event2.put("event_start", new Date());
		events.add(event2);

		return events;
	}

	/**
	 * Retrieve the results for a single event
	 * @param eventId the key of the event
	 * @return a hashmap for each result in the event.
	 * @throws OlympicsDBException
	 */
	ArrayList<HashMap<String, Object>> getResultsOfEvent(Integer eventId) throws OlympicsDBException {
		// FIXME: Replace the following with REAL OPERATIONS!

		ArrayList<HashMap<String, Object>> results = new ArrayList<>();


		HashMap<String,Object> result1 = new HashMap<String,Object>();
		result1.put("participant", "The Frog, Kermit");
		result1.put("country_name", "Fraggle Rock");
		result1.put("medal", "Gold");
		results.add(result1);

		HashMap<String,Object> result2 = new HashMap<String,Object>();
		result2.put("participant", "Cirus, Miley");
		result2.put("country_name", "United States");
		result2.put("medal", "Silver");
		results.add(result2);

		HashMap<String,Object> result3 = new HashMap<String,Object>();
		result3.put("participant", "Bond, James");
		result3.put("country_name", "Great Britain");
		result3.put("medal", "Bronze");
		results.add(result3);

		HashMap<String,Object> result4 = new HashMap<String,Object>();
		result4.put("participant", "McKenzie, Namor");
		result4.put("country_name", "Atlantis");
		result4.put("medal", null);
		results.add(result4);

		return results;
	}


	///////   Journeys    ////////

	/**
	 * Array list of journeys from one place to another on a given date
	 * @param journeyDate the date of the journey
	 * @param fromPlace the origin, starting place.
	 * @param toPlace the destination, place to go to.
	 * @return a list of all journeys from the origin to destination
	 */
	ArrayList<HashMap<String, Object>> findJourneys(String fromPlace, String toPlace, Date journeyDate) throws OlympicsDBException {
		// FIXME: Replace the following with REAL OPERATIONS!
		ArrayList<HashMap<String, Object>> journeys = new ArrayList<>();
		
		
		
		// TODO REMOVE
		//fromPlace = "Athletes Village";
		//toPlace = "Sydney Olympic Park, Olympic Stadium";
		///
		
		System.out.println(journeyDate.toString());
		// operations to convert date into a day range. 
		
		String date1 = "";
		String date2 = "";
		
		// create date in a string format and use midnight to start
		// find the date after the current date and use midnight for then. 
		// use the latest query from 
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
		
		//int day = Integer.parseInt(sdf.format(journeyDate));

		date1 += sdf.format(journeyDate)+" 12:00:00 AM";
		System.out.println(date1);
		Calendar cal = Calendar.getInstance();
        cal.setTime(journeyDate);
        cal.add(Calendar.DATE, 1); //minus number would decrement the days
        Date next_day = cal.getTime();
		date2+=sdf.format(next_day)+" 12:00:00 AM";
		System.out.println(date2);
        
		String query = "select P1.place_name as depart_from, P2.place_name as arrive_to,journey_id,vehicle_code,depart_time,arrivetime,nbooked, capacity"
				+ " from Journey join Place P1 on(from_place = P1.place_id)"
				+" join Place P2 on(to_place = P2.place_id) join Vehicle using(vehicle_code) "
				+ " where P1.place_name = ? and P2.place_name = ? and depart_time between ? and ?";
		PreparedStatement stmt = null;
		
		try{
			Connection conn = getConnection();
			stmt = conn.prepareStatement(query);
			stmt.setString(1,fromPlace);
			stmt.setString(2,toPlace);
			stmt.setString(3,date1);
			stmt.setString(4,date2);
			ResultSet rs = stmt.executeQuery();
			int nr = 0;
			int journey_id = 0;
			while (rs.next()){	
				
				journeys.add(createJourneyTuples(Integer.parseInt(rs.getString("journey_id")),
						rs.getString("vehicle_code"),rs.getString("depart_from"),rs.getString("arrive_to"),
						rs.getDate("depart_time"),rs.getDate("arrivetime"),
						Integer.parseInt(rs.getString("capacity"))-Integer.parseInt(rs.getString("nbooked"))));
				
				
				
				
				
				//journey_id = Integer.parseInt(rs.getString("journey_id"));
				//HashMap<String,Object> journey1 = new HashMap<String,Object>();
				/*journeys.set(nr, new HashMap<String,Object>());
				journeys.set("journey_id",Integer.valueOf(journey_id));
				journey1.put("vehicle_code",rs.getString("vehicle_code"));
				journey1.put("origin_name",rs.getString("depart_from"));
				journey1.put("dest_name",rs.getString("arrive_to"));
				journey1.put("when_departs",rs.getString("depart_time"));
				journey1.put("when_arrives",rs.getString("arrivetime"));
				journey1.put("available_seats",rs.getString("nbooked"));
				*/
				
				
				//journey1.clear();
				System.out.println(journey_id);
				System.out.println(journeys.get(nr).get("journey_id").toString());
				System.out.println(journeys.get(nr).get("vehicle_code").toString());
				System.out.println(journeys.get(nr).get("origin_name").toString());
				System.out.println(journeys.get(nr).get("dest_name").toString());
				System.out.println(journeys.get(nr).get("when_departs").toString());
				System.out.println(journeys.get(nr).get("when_arrives").toString());
				nr++;
			}
			System.out.println(journeys.size());
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Error checking login details", e);
		}
		return journeys;
	}
	
	
	public static HashMap<String,Object> createJourneyTuples(int journey_id, String vehicle_code, String depart_from, String arrive_to, Date when_departs, Date when_arrives, int availability){
		HashMap<String,Object> journey1 = new HashMap<String,Object>();
		journey1.put("journey_id",journey_id);
		journey1.put("vehicle_code",vehicle_code);
		journey1.put("origin_name",depart_from);
		journey1.put("dest_name",arrive_to);
		journey1.put("when_departs",when_departs);
		journey1.put("when_arrives",when_arrives);
		journey1.put("available_seats",availability);
		return journey1;
	}

	ArrayList<HashMap<String,Object>> getMemberBookings(String memberID) throws OlympicsDBException {
		ArrayList<HashMap<String,Object>> bookings = new ArrayList<HashMap<String,Object>>();

		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		HashMap<String,Object> bookingex1 = new HashMap<String,Object>();
		bookingex1.put("journey_id", Integer.valueOf(17));
		bookingex1.put("vehicle_code", "XYZ124");
		bookingex1.put("origin_name", "SIT");
		bookingex1.put("dest_name", "Olympic Park");
		bookingex1.put("when_departs", new Date());
		bookingex1.put("when_arrives", new Date());
		bookings.add(bookingex1);

		HashMap<String,Object> bookingex2 = new HashMap<String,Object>();
		bookingex2.put("journey_id", Integer.valueOf(25));
		bookingex2.put("vehicle_code", "ABC789");
		bookingex2.put("origin_name", "Olympic Park");
		bookingex2.put("dest_name", "Sydney Airport");
		bookingex2.put("when_departs", new Date());
		bookingex2.put("when_arrives", new Date());
		bookings.add(bookingex2);

		return bookings;
	}

	/**
	 * Get details for a specific journey
	 * 
	 * @return Various details of journey - see JourneyDetails.java
	 * @throws OlympicsDBException
	 */
	public HashMap<String,Object> getJourneyDetails(int bay) throws OlympicsDBException {
		// FIXME: REPLACE FOLLOWING LINES WITH REAL OPERATION
		// See the constructor in BayDetails.java
		HashMap<String,Object> details = new HashMap<String,Object>();

		details.put("journey_id", Integer.valueOf(17));
		details.put("vehicle_code", "XYZ124");
		details.put("origin_name", "SIT");
		details.put("dest_name", "Olympic Park");
		details.put("when_departs", new Date());
		details.put("when_arrives", new Date());
		details.put("capacity", Integer.valueOf(6));
		details.put("nbooked", Integer.valueOf(3));

		return details;
	}

	public HashMap<String,Object> makeBooking(String byStaff, String forMember,String Vehicle, Date departs) throws OlympicsDBException {
		HashMap<String,Object> booking = null;

		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		booking = new HashMap<String,Object>();
		booking.put("vehicle", "TR870R");
		booking.put("start_day", "21/12/2020");
		booking.put("start_time", new Date());
		booking.put("to", "SIT");
		booking.put("from", "Wentworth");
		booking.put("booked_by", "Mike");
		booking.put("whenbooked", new Date());
		return booking;
	}

	public HashMap<String,Object> getBookingDetails(String memberID, Integer journeyId) throws OlympicsDBException {
		HashMap<String,Object> booking = null;

		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		booking = new HashMap<String,Object>();

		booking.put("journey_id", journeyId);
		booking.put("vehicle_code", "TR870R");
		booking.put("when_departs", new Date());
		booking.put("dest_name", "SIT");
		booking.put("origin_name", "Wentworth");
		booking.put("bookedby_name", "Mrs Piggy");
		booking.put("bookedfor_name", "Mike");
		booking.put("when_booked", new Date());
		booking.put("when_arrives", new Date());


		return booking;
	}

	public ArrayList<HashMap<String, Object>> getSports() throws OlympicsDBException {
		ArrayList<HashMap<String,Object>> sports = new ArrayList<HashMap<String,Object>>();

		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		HashMap<String,Object> sport1 = new HashMap<String,Object>();
		sport1.put("sport_id", Integer.valueOf(1));
		sport1.put("sport_name", "Chillaxing");
		sport1.put("discipline", "Couch Potatoing");
		sports.add(sport1);

		HashMap<String,Object> sport2 = new HashMap<String,Object>();
		sport2.put("sport_id", Integer.valueOf(2));
		sport2.put("sport_name", "Frobnicating");
		sport2.put("discipline", "Tweaking");
		sports.add(sport2);

		HashMap<String,Object> sport3 = new HashMap<String,Object>();
		sport3.put("sport_id", Integer.valueOf(3));
		sport3.put("sport_name", "Frobnicating");
		sport3.put("discipline", "Fiddling");
		sports.add(sport3);

		return sports;
	}


	/////////////////////////////////////////
	/// Functions below don't need
	/// to be touched.
	///
	/// They are for connecting and handling errors!!
	/////////////////////////////////////////

	/**
	 * Default constructor that simply loads the JDBC driver and sets to the
	 * connection details.
	 *
	 * @throws ClassNotFoundException if the specified JDBC driver can't be
	 * found.
	 * @throws OlympicsDBException anything else
	 */
	DatabaseBackend(InputStream config) throws ClassNotFoundException, OlympicsDBException {
		Properties props = new Properties();
		try {
			props.load(config);
		} catch (IOException e) {
			throw new OlympicsDBException("Couldn't read config data",e);
		}

		dbUser = props.getProperty("username");
		dbPass = props.getProperty("userpass");
		String port = props.getProperty("port");
		String dbname = props.getProperty("dbname");
		String server = props.getProperty("address");;

		// Load JDBC driver and setup connection details
		String vendor = props.getProperty("dbvendor");
		if(vendor==null) {
			throw new OlympicsDBException("No vendor config data");
		} else if ("postgresql".equals(vendor)) { 
			Class.forName("org.postgresql.Driver");
			connstring = "jdbc:postgresql://" + server + ":" + port + "/" + dbname;
		} else if ("oracle".equals(vendor)) {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			connstring = "jdbc:oracle:thin:@" + server + ":" + port + ":" + dbname;
		} else throw new OlympicsDBException("Unknown database vendor: " + vendor);

		// test the connection
		Connection conn = null;
		try {
			conn = getConnection();
		} catch (SQLException e) {
			throw new OlympicsDBException("Couldn't open connection", e);
		} finally {
			reallyClose(conn);
		}
	}

	/**
	 * Utility method to ensure a connection is closed without 
	 * generating any exceptions
	 * @param conn Database connection
	 */
	private void reallyClose(Connection conn) {
		if(conn!=null)
			try {
				conn.close();
			} catch (SQLException ignored) {}
	}

	/**
	 * Construct object with open connection using configured login details
	 * @return database connection
	 * @throws SQLException if a DB connection cannot be established
	 */
	private Connection getConnection() throws SQLException {
		Connection conn;
		conn = DriverManager.getConnection(connstring, dbUser, dbPass);
		return conn;
	}



}
