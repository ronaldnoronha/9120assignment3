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
		String query = "select event_name,EVENT_GENDER, Place_name, EVENT_START , EVENT.EVENT_ID, sport_id  "
				   + " from event"
				   + "  inner join sport  using (sport_id)" 
				   + " inner join place on (sport_venue = place_id)"
				   + "where   sport_id = ?";
		Connection conn = null;
		ArrayList<HashMap<String, Object>> events = new ArrayList<>();
		try {

			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);
			stmt.setInt(1, sportname);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				HashMap<String, Object> event1 = new HashMap<String, Object>();
				event1.put("event_id",rs.getInt("EVENT_ID"));
				event1.put("sport_id",rs.getInt("sport_id"));
				event1.put("event_name", rs.getString("event_name"));
				event1.put("event_gender", rs.getString("EVENT_GENDER"));
				event1.put("sport_venue", rs.getString("Place_name"));
				event1.put("event_start",rs.getDate("EVENT_START"));
				events.add(event1);

			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			reallyClose(conn);
		}

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
		String query = "SELECT MEMBER.GIVEN_NAMES, MEMBER.FAMILY_NAME, COUNTRY.country_name, PARTICIPATES.medal "+
				" FROM event "+
				" JOIN PARTICIPATES on EVENT.event_id = Participates.event_id"+
				" JOIN MEMBER on MEMBER.member_id = PARTICIPATES.ATHLETE_ID"+
				" JOIN COUNTRY on COUNTRY.COUNTRY_CODE = MEMBER.COUNTRY_CODE"+
				" WHERE event.event_id = ?";
		Connection conn = null;
		ArrayList<HashMap<String, Object>> results = new ArrayList<>();
		
		try {

			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);
			stmt.setInt(1, eventId);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {

		HashMap<String, Object> result1 = new HashMap<String, Object>();
		result1.put("participant",rs.getString("Given_names") + " " + rs.getString("FAMILY_NAME"));
		result1.put("country_name",rs.getString("Country_name"));
		result1.put("medal",rs.getString("medal"));
		results.add(result1);

			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			reallyClose(conn);
		}

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
	
		// operations to convert date into a day range. 
		
		String date1 = "";
		String date2 = "";
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
		date1 += sdf.format(journeyDate)+" 12:00:00 AM";
		Calendar cal = Calendar.getInstance();
        cal.setTime(journeyDate);
        cal.add(Calendar.DATE, 1); 
        Date next_day = cal.getTime();
		date2+=sdf.format(next_day)+" 12:00:00 AM";
        
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
				
				nr++;
			}
			System.out.println(journeys.size());
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Error finding journeys", e);
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
	ArrayList<HashMap<String, Object>> getMemberBookings(String memberID) throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> bookings = new ArrayList<HashMap<String, Object>>();
		// browse booking
		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED

		String query = "select journey_id, vehicle_code, arrivetime, depart_time," + " f.place_name fromPlace,"
				+ " t.place_name toPlace" + " from booking join journey using (journey_id)"
				+ " inner join place f on from_place = f.place_id" + " inner join place t on to_place = t.place_id"
				+ " where booked_for = ? ORDER BY depart_time DESC";
		Connection conn = null;
		try {

			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);
			stmt.setString(1, memberID);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				HashMap<String, Object> booking = new HashMap<String, Object>();

				booking.put("journey_id", rs.getInt("journey_id"));
				booking.put("vehicle_code", rs.getString("vehicle_code"));
				booking.put("origin_name", rs.getString("fromPlace"));
				booking.put("dest_name", rs.getString("toPlace"));
				booking.put("when_departs", rs.getDate("depart_time"));
				booking.put("when_arrives", rs.getDate("arrivetime"));

				bookings.add(booking);
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			reallyClose(conn);
		}

		return bookings;
	}
	/**
	 * Get details for a specific journey
	 * 
	 * @return Various details of journey - see JourneyDetails.java
	 * @throws OlympicsDBException
	 */
	public HashMap<String,Object> getJourneyDetails(int journeyId) throws OlympicsDBException {
		// FIXME: REPLACE FOLLOWING LINES WITH REAL OPERATION
		// See the constructor in BayDetails.java
		HashMap<String,Object> details = new HashMap<String,Object>();
		String query = "select P1.place_name as depart_from, P2.place_name as arrive_to,journey_id,vehicle_code,depart_time,arrivetime,nbooked, capacity"
				+ " from Journey join Place P1 on(from_place = P1.place_id)"
				+" join Place P2 on(to_place = P2.place_id) join Vehicle using(vehicle_code) "
				+ " where journey_id = ?";
		PreparedStatement stmt = null;
		
		try{
			Connection conn = getConnection();
			stmt = conn.prepareStatement(query);
			stmt.setInt(1,journeyId);
			ResultSet rs = stmt.executeQuery();
			int nr = 0;	
			
			while (rs.next()){	
				details.put("journey_id", Integer.valueOf(journeyId));
				details.put("vehicle_code",rs.getString("vehicle_code"));
				details.put("origin_name",rs.getString("depart_from"));
				details.put("dest_name",rs.getString("arrive_to"));
				details.put("when_departs",rs.getDate("depart_time"));
				details.put("when_arrives",rs.getDate("arrivetime"));
				details.put("capacity",rs.getInt("capacity"));
				details.put("nbooked",rs.getInt("nbooked"));
				/*journeys.add(createJourneyTuples(Integer.parseInt(rs.getString("journey_id")),
						rs.getString("vehicle_code"),rs.getString("depart_from"),rs.getString("arrive_to"),
						rs.getDate("depart_time"),rs.getDate("arrivetime"),
						Integer.parseInt(rs.getString("capacity"))-Integer.parseInt(rs.getString("nbooked"))));
				
				System.out.println(journey_id);
				System.out.println(journeys.get(nr).get("journey_id").toString());
				System.out.println(journeys.get(nr).get("vehicle_code").toString());
				System.out.println(journeys.get(nr).get("origin_name").toString());
				System.out.println(journeys.get(nr).get("dest_name").toString());
				System.out.println(journeys.get(nr).get("when_departs").toString());
				System.out.println(journeys.get(nr).get("when_arrives").toString());
			*/	nr++;
			}
			//System.out.println(journeys.size());
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Error checking login details", e);
		}
		
		
		/*details.put("journey_id", Integer.valueOf(journeyID));
		details.put("vehicle_code", "XYZ124");
		details.put("origin_name", "SIT");
		details.put("dest_name", "Olympic Park");
		details.put("when_departs", new Date());
		details.put("when_arrives", new Date());
		details.put("capacity", Integer.valueOf(6));
		details.put("nbooked", Integer.valueOf(3));*/

		return details;
	}

	public HashMap<String,Object> makeBooking(String byStaff, String forMember,String Vehicle, Date departs) throws OlympicsDBException {
		HashMap<String,Object> booking = null;
		booking = new HashMap<String,Object>();
		System.out.println(byStaff);
		System.out.println(departs.toString());
		
		PreparedStatement stmt  = null;
		String query = "select * from staff where member_id = ?";
		boolean staffExists = false;
		boolean availability = false;
		try
		{
			Connection conn = getConnection();
			stmt  = conn.prepareStatement(query);
			stmt.setString(1,byStaff);
			ResultSet rs = stmt.executeQuery(); 
			if (rs.next()){
				staffExists = true;				
			}
			rs.close();
			stmt.close();
			conn.close();
		}
		catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Error checking Staff", e);
		}
		query = "select capacity, nbooked, journey_id from Journey join vehicle using(vehicle_code) where vehicle_code  = ? and depart_time = ?";
		int journeyId = 0;
		int nbooked = 0;
		String date1 = "";
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy hh:mm:ss a");
		date1 += sdf.format(departs);
		System.out.println(date1);			
		
		try
		{
			Connection conn = getConnection();
			stmt  = conn.prepareStatement(query);
			stmt.setString(1,Vehicle);
			stmt.setString(2,date1);
			ResultSet rs = stmt.executeQuery(); 
			if (rs.next()){
				if (rs.getInt("nbooked")<rs.getInt("capacity")){
					availability = true;
					System.out.println("Availability"+availability);
				}
				nbooked = rs.getInt("nbooked");
				journeyId = rs.getInt("journey_id");
			}
			rs.close();
			stmt.close();
			conn.close();
		}
		catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Error checking availability", e);
		}
		
		
		query = "insert into booking values(?,?,current_timestamp,?)";
		try
		{
			Connection conn = getConnection();
			stmt  = conn.prepareStatement(query);
			stmt.setString(1,forMember);
			stmt.setString(2,byStaff);
			stmt.setInt(3,journeyId);
			
			System.out.println(forMember);
			System.out.println(byStaff);
			System.out.println(journeyId);
			ResultSet rs = stmt.executeQuery();
			System.out.println("booking made");		
			rs.close();
			stmt.close();
			conn.close();
		}
		catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Booking couldnt be made", e);
		}
		
		/*Statement stmt1 = null;
		try {
			Connection conn = getConnection();
	        stmt1 = conn.createStatement(
	            ResultSet.TYPE_SCROLL_SENSITIVE,
	            ResultSet.CONCUR_UPDATABLE);

	        ResultSet uprs = stmt1.executeQuery(
	            "select * from booking");

	        uprs.moveToInsertRow();
	     	uprs.updateInt("journey_id", journeyId);
	     	uprs.updateString("booked_for",forMember);
	        uprs.updateString("booked_by",byStaff);
	        uprs.updateString("when_booked",date1);
	        uprs.insertRow();
	        uprs.beforeFirst();
	        System.out.println("booking made");
	        uprs.close();
			conn.close();
			stmt1.close();
	    } catch (Exception e ) {
	    	System.err.println(e);
			throw new OlympicsDBException("Booking couldnt be made", e);
	    } 
*/		
		
		
		
		query = "update journey set nbooked = ? where journey_id = ? and depart_time = ?";
		try
		{
			Connection conn = getConnection();
			stmt  = conn.prepareStatement(query);
			stmt.setInt(1,nbooked+1);
			stmt.setInt(2,journeyId);
			stmt.setString(3,date1);
			
			ResultSet rs = stmt.executeQuery(); 
			//if (rs.next()){
				System.out.println("Journey updated");		
			//}
			rs.close();
			stmt.close();
			conn.close();
		}
		catch (Exception e) {
			System.err.println(e);
			throw new OlympicsDBException("Journey couldnt be updated", e);
		}
		

		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		
		booking.put("vehicle", "TR870R");
		booking.put("start_day", "21/12/2020");
		booking.put("start_time", new Date());
		booking.put("to", "SIT");
		booking.put("from", "Wentworth");
		booking.put("booked_by", "Mike");
		booking.put("whenbooked", new Date());
		return booking;
	}

	public HashMap<String, Object> getBookingDetails(String memberID, Integer journeyId) throws OlympicsDBException {
		HashMap<String, Object> booking = null;

		String query = "SELECT journey.journey_id , journey.DEPART_TIME," + " t.place_name toPlace, "
				+ " f.place_name fromPlace, " + " journey.VEHICLE_CODE, "
				+ " member.TITLE || ' ' ||  member.GIVEN_NAMES || ' ' || member.FAMILY_NAME bookedFor,"
				+ " staff.TITLE || ' ' ||  staff.GIVEN_NAMES || ' ' || staff.FAMILY_NAME bookedBy,"
				+ " booking.WHEN_BOOKED," + " journey.arrivetime" + " FROM journey"
				+ " INNER JOIN vehicle on vehicle.VEHICLE_CODE = journey.VEHICLE_CODE"
				+ " INNER JOIN place t on journey.to_place = t.place_id"
				+ " INNER JOIN place f on journey.from_place = f.place_id   "
				+ " INNER JOIN booking on journey.journey_id = booking.journey_id"
				+ " INNER JOIN member on booking.BOOKED_FOR = member.MEMBER_ID"
				+ " INNER JOIN member staff on booking.BOOKED_BY = staff.MEMBER_ID"
				+ " WHERE journey.journey_id = ? AND booking.BOOKED_FOR = ?";
		Connection conn = null;
		try {

			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);
			stmt.setInt(1, journeyId);
			stmt.setString(2, memberID);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				booking = new HashMap<String, Object>();

				booking.put("journey_id", rs.getInt("journey_id"));
				booking.put("vehicle_code", rs.getString("VEHICLE_CODE"));
				booking.put("when_departs", rs.getDate("DEPART_TIME"));
				booking.put("dest_name", rs.getString("toPlace"));
				booking.put("origin_name", rs.getString("fromPlace"));
				booking.put("bookedby_name", rs.getString("bookedBy"));
				booking.put("bookedfor_name", rs.getString("bookedFor"));
				booking.put("when_booked", rs.getDate("WHEN_BOOKED"));
				booking.put("when_arrives", rs.getDate("arrivetime"));
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			reallyClose(conn);
		}

		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		/*
		 * booking = new HashMap<String,Object>();
		 * 
		 * booking.put("journey_id", journeyId); booking.put("vehicle_code",
		 * "TR870R"); booking.put("when_departs", new Date());
		 * booking.put("dest_name", "SIT"); booking.put("origin_name",
		 * "Wentworth"); booking.put("bookedby_name", "Mrs Piggy");
		 * booking.put("bookedfor_name", "Mike"); booking.put("when_booked", new
		 * Date()); booking.put("when_arrives", new Date());
		 */

		return booking;
	}
	public ArrayList<HashMap<String, Object>> getSports() throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> sports = new ArrayList<HashMap<String, Object>>();

		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		String query = "SELECT SPORT.SPORT_ID, SPORT.SPORT_NAME, SPORT.DISCIPLINE FROM SPORT ORDER BY SPORT_NAME ASC";
		Connection conn = null;
		try {

			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);
			
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				HashMap<String, Object> sport1 = new HashMap<String, Object>();
				sport1.put("sport_id", rs.getInt("SPORT_ID"));
				sport1.put("sport_name", rs.getString("SPORT_NAME"));
				sport1.put("discipline", rs.getString("DISCIPLINE"));
				sports.add(sport1);
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			reallyClose(conn);
		}

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
