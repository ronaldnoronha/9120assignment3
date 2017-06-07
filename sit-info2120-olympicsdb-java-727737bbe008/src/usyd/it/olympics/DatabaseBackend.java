package usyd.it.olympics;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.io.Closeable;
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
		Connection conn = null;
		try {
			conn = getConnection();
			PreparedStatement stmt = null;
			String pass_word = new String(password);
			String query = "select title, given_names, family_name, place_name,"
					+ "country_name "
					+ "from Member join place on(accommodation=place_id) "
					+ "join country using(country_code) "
					+ "where member_id = ? and pass_word = ?";
			stmt = conn.prepareStatement(query);
			stmt.setString(1,member);
			stmt.setString(2, pass_word);
			ResultSet rs = stmt.executeQuery();
			boolean loggedin = false;
			while (rs.next()){
				details = new HashMap<String,Object>();
				loggedin = true;
				details.put("member_id", member);
				details.put("title", rs.getString("title"));			
				details.put("first_name",rs.getString("given_names"));
				details.put("family_name",rs.getString("family_name"));
				details.put("residence",rs.getString("place_name"));
				details.put("country_name",rs.getString("country_name"));
			}
			if (loggedin){
				String[] member_types = {"athlete", "official", "staff"};
				for (int i = 0;i<member_types.length;i++){	
					query = "select member_id from "+ member_types[i]+" where member_id = ?";				
					stmt = conn.prepareStatement(query);
					stmt.setString(1,member);
					rs = stmt.executeQuery();
					if(rs.next()){
						if (details.get("member_type")==null){
							details.put("member_type",member_types[i]);
						}else{
							details.put("member_type",details.get("member_type")+", "+member_types[i]);						
						}					
					}				
				}
			}

			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {			
			throw new OlympicsDBException("Error checking login details" + e);
		} finally {
			if (conn!=null){
				reallyClose(conn);
			}
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
		HashMap<String, Object> details = null;
		String query = "";
		PreparedStatement stmt = null;
		Connection conn = null;
		try {
			
			conn = getConnection();

			String[] member_types = {"athlete", "official", "staff"};
			for (int i = 0;i<member_types.length;i++){	
				query = "select member_id from "+ member_types[i]+" where member_id = ?";				
				stmt = conn.prepareStatement(query);
				stmt.setString(1,memberID);
				ResultSet rs = stmt.executeQuery();
				if(rs.next()){
					if (details==null){
						details = new HashMap<String, Object>();
						details.put("member_type",member_types[i]);
					}else{
						details.put("member_type",details.get("member_type")+", "+member_types[i]);						
					}					
				}				
			}
			if (details!=null){
				query = "select title, given_names, family_name, place_name,"
						+ "country_name "
						+ "from Member join place on(accommodation=place_id) "
						+ "join country using(country_code) "
						+ "where member_id = ?";
				stmt = conn.prepareStatement(query);
				stmt.setString(1,memberID);
				ResultSet rs = stmt.executeQuery();

				while (rs.next()){
					details.put("member_id", memberID);
					details.put("title", rs.getString("title"));			
					details.put("first_name",rs.getString("given_names"));
					details.put("family_name",rs.getString("family_name"));
					details.put("residence",rs.getString("place_name"));
					details.put("country_name",rs.getString("country_name"));
				}
				int bronze = 0;
				int silver = 0;
				int gold = 0;
				if (details.get("member_type").toString().contains("athlete")){
					query = "select count(*) as num_medal, medal "
							+ "from participates "
							+ "where medal is not null and athlete_id = ?"
							+ "group by medal";			
					stmt = conn.prepareStatement(query);
					stmt.setString(1,memberID);
					rs = stmt.executeQuery();
					while (rs.next()){												
						if (rs.getString("medal").equals("B")){
							bronze=rs.getInt("num_medal");
						}
						else if (rs.getString("medal").equals("S")){
							silver=rs.getInt("num_medal");
						}
						else if (rs.getString("medal").equals("G")){
							gold=rs.getInt("num_medal");
						}
					}
					rs.close();
					stmt.close();
					// team medals
					query  ="select count(*) as num_medal,medal "
							+ "from team natural join teammember "
							+ "where medal is not null and athlete_id = ?"
							+ "group by medal";
					stmt = conn.prepareStatement(query);
					stmt.setString(1,memberID);				
					rs = stmt.executeQuery();				
					while (rs.next()){					
						if (rs.getString("medal").equals("B")){
							bronze+=rs.getInt("num_medal");
						}
						else if (rs.getString("medal").equals("S")){
							silver+=rs.getInt("num_medal");
						}
						else if (rs.getString("medal").equals("G")){
							gold+=rs.getInt("num_medal");
						}
					}
					details.put("num_gold", Integer.valueOf(gold));
					details.put("num_silver", Integer.valueOf(silver));
					details.put("num_bronze", Integer.valueOf(bronze));
				}else{
					details.put("num_gold",null);
					details.put("num_silver",null);
					details.put("num_bronze",null);
				}
				query = "select count(*) as c from Booking where booked_for = ?";
				stmt = conn.prepareStatement(query);			
				stmt.setString(1, memberID);
				rs = stmt.executeQuery();
				while (rs.next()){			
					details.put("num_bookings",rs.getInt("c"));
				}
				rs.close();
			}
			stmt.close();			
			conn.close();
			return details;
		} catch (Exception e) {
			throw new OlympicsDBException("Error checking Member Details " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
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

		String query = "select event_id, sport_id, event_name, event_gender, "
				+ "place_name, event_start "
				+ "from sport join event using(sport_id) "
				+ "join place on(sport_venue=place_id)"
				+ "where sport_id = ?";
		Connection conn = null;
		ArrayList<HashMap<String, Object>> events = new ArrayList<>();
		try {

			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);
			stmt.setInt(1, sportname);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				HashMap<String, Object> event1 = new HashMap<String, Object>();
				event1.put("event_id",rs.getInt("event_id"));
				event1.put("sport_id",rs.getInt("sport_id"));
				event1.put("event_name", rs.getString("event_name"));
				event1.put("event_gender", rs.getString("event_gender"));
				event1.put("sport_venue", rs.getString("place_name"));
				event1.put("event_start",rs.getDate("event_start"));
				events.add(event1);
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Error event of sports " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
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

		String query = "select count(*) as c "
				+ "from individualevent where event_id = ?";
		Connection conn = null;
		ArrayList<HashMap<String, Object>> results = new ArrayList<>();
		HashMap<String, Object> result1 = null;
		try {
			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);
			stmt.setInt(1, eventId);
			ResultSet rs = stmt.executeQuery();
			boolean individual = false;
			while (rs.next()){
				if (rs.getInt("c")>0){
					individual = true;
				}
			}
			if (individual) {
				rs.close();
				stmt.close();
				query = "select * from participates "
						+ "join member on(athlete_id=member_id) "
						+ "join country using(country_code) "
						+ "where event_id = ?"
						+ "order by family_name,given_names,country_name ";
				stmt = conn.prepareStatement(query);
				stmt.setInt(1, eventId);
				rs = stmt.executeQuery();

				while (rs.next()){
					result1 = new HashMap<String, Object>();
					result1.put("participant",rs.getString("family_name") + ", " + rs.getString("given_names"));
					String country_name = rs.getString("Country_name");
					if (country_name==null){
						result1.put("country_name","Unified Team");						
					}
					else {
						result1.put("country_name",country_name);
					}

					String medal = rs.getString("medal");					
					if (medal==null) {
						result1.put("medal",null);
					}
					else if (medal.equals("B")) {
						result1.put("medal","Bronze");
					}
					else if (medal.equals("S")){
						result1.put("medal","Silver");
					}
					else if (medal.equals("G")){
						result1.put("medal","Gold");
					}
					results.add(result1);					
				}
			}
			// team event check
			else {
				query = "select * from Team join country using(country_code)"
						+ "where event_id = ?"
						+ "order by team_name,country_name";
				rs.close();
				stmt.close();
				stmt = conn.prepareStatement(query);
				stmt.setInt(1, eventId);
				System.out.println(true);
				rs = stmt.executeQuery();

				while (rs.next()){

					result1 = new HashMap<String, Object>();
					result1.put("participant",rs.getString("team_name"));
					String country_name = rs.getString("Country_name");
					if (country_name==null){
						result1.put("country_name","Unified Team");						
					}
					else {
						result1.put("country_name",country_name);
					}

					String medal = rs.getString("medal");					
					if (medal==null) {
						result1.put("medal",null);
					}
					else if (medal.equals("B")) {
						result1.put("medal","Bronze");
					}
					else if (medal.equals("S")){
						result1.put("medal","Silver");
					}
					else if (medal.equals("G")){
						result1.put("medal","Gold");
					}
					results.add(result1);					
				}


			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Error in results of events " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
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
		ArrayList<HashMap<String, Object>> journeys = new ArrayList<>();
		HashMap<String,Object> journey1 = null;
		String date1 = "";
		String date2 = "";
		System.out.println(journeyDate.toString());
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
		date1 += sdf.format(journeyDate)+" 12:00:00 AM";
		Calendar cal = Calendar.getInstance();
		cal.setTime(journeyDate);
		cal.add(Calendar.DATE, 1); 
		Date next_day = cal.getTime();
		date2+=sdf.format(next_day)+" 12:00:00 AM";

		String query = "select P1.place_name as depart_from, P2.place_name as arrive_to,journey_id,"
				+ "vehicle_code,depart_time,arrive_time,nbooked, capacity"
				+ " from Journey join Place P1 on(from_place = P1.place_id)"
				+" join Place P2 on(to_place = P2.place_id) join Vehicle using(vehicle_code) "
				+ " where P1.place_name = ? and P2.place_name = ? and depart_time between ? and ?";
		PreparedStatement stmt = null;
		Connection conn = null;
		try{
			conn = getConnection();
			stmt = conn.prepareStatement(query);
			stmt.setString(1,fromPlace);
			stmt.setString(2,toPlace);
			stmt.setString(3,date1);
			stmt.setString(4,date2);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()){	
				journey1 = new HashMap<String,Object>();
				journey1.put("journey_id",rs.getInt("journey_id"));
				journey1.put("vehicle_code",rs.getString("vehicle_code"));
				journey1.put("origin_name",rs.getString("depart_from"));
				journey1.put("dest_name",rs.getString("arrive_to"));
				journey1.put("when_departs",rs.getTimestamp("depart_time"));
				journey1.put("when_arrives",rs.getTimestamp("arrive_time"));
				journey1.put("available_seats",Integer.parseInt(rs.getString("capacity"))-Integer.parseInt(rs.getString("nbooked")));
				journeys.add(journey1);
				/*journeys.add(createJourneyTuples(Integer.parseInt(rs.getString("journey_id")),
						rs.getString("vehicle_code"),rs.getString("depart_from"),rs.getString("arrive_to"),
						rs.getDate("depart_time"),rs.getDate("arrivetime"),
						Integer.parseInt(rs.getString("capacity"))-Integer.parseInt(rs.getString("nbooked"))));*/
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Error finding journeys " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
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
	ArrayList<HashMap<String, Object>> getMedalTally() throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> medals = new ArrayList<HashMap<String, Object>>();
		String query = "select country_name, count(*) as num_medals "
				+ "from participates join member on(athlete_id=member_id) "
				+ "join country using(country_code) "
				+ "where medal is not null "
				+ "group by country_name "
				+ "order by (num_medals) desc";
		HashMap<String,Object> medal1 = null;

		Connection conn = null;
		try {
			conn = getConnection();
			PreparedStatement stmt = conn.prepareStatement(query);			
			ResultSet rs = stmt.executeQuery();
			String country_name = "";
			while (rs.next()){
				medal1 = new HashMap<String,Object>();				
				country_name = rs.getString("country_name");
				if (country_name==null){
					country_name = "Unified Team";
				}
				medal1.put("country_name", country_name);
				medal1.put("gold", "0");
				medal1.put("silver","0");
				medal1.put("bronze","0");
				medal1.put("total",rs.getString("num_medals"));
				medals.add(medal1);
			}
			stmt.close();
			rs.close();
			query = "select country_name, count(*) as num_medals "
					+ "from country join team using(country_code) "
					+ "where medal is not null "
					+ "group by country_name";			
			stmt = conn.prepareStatement(query);			
			rs = stmt.executeQuery();
			boolean countryOnList = false;
			int num_medals;
			while (rs.next()){								
				country_name = rs.getString("country_name");
				if (country_name==null){
					country_name = "Unified Team";
				}
				// check if team already on the list
				countryOnList = false;
				for (int i=0;i<medals.size();i++){
					if (medals.get(i).get("country_name").toString().equals(country_name)){
						countryOnList = true;
						num_medals = rs.getInt("num_medals")+Integer.parseInt(medals.get(i).get("total").toString());						
					}
				}
				if (!countryOnList){
					medal1 = new HashMap<String,Object>();
					medal1.put("country_name", country_name);
					medal1.put("gold", "0");
					medal1.put("silver","0");
					medal1.put("bronze","0");
					medal1.put("total",rs.getString("num_medals"));
					medals.add(medal1);
				}				
			}
			stmt.close();
			rs.close();


			query = "select country_name, medal, count(*) as num_medals "
					+ "from participates join member on(athlete_id=member_id) "
					+ "join country using(country_code) "
					+ "where medal is not null and medal = 'G' "
					+ "group by country_name, medal "
					+ "order by (num_medals) desc";
			stmt = conn.prepareStatement(query);			
			rs = stmt.executeQuery();

			while (rs.next()){
				country_name = rs.getString("country_name");
				if (country_name==null){
					country_name = "Unified Team";
				}				
				for (int i=0;i<medals.size();i++){
					if (medals.get(i).get("country_name").toString().equals(country_name)){
						medals.get(i).put("gold",rs.getString("num_medals"));						
					}
				}	
			}
			query = "select country_name, medal, count(*) as num_medals "
					+ "from participates join member on(athlete_id=member_id) "
					+ "join country using(country_code) "
					+ "where medal is not null and medal = 'S' "
					+ "group by country_name, medal "
					+ "order by (num_medals) desc";
			stmt = conn.prepareStatement(query);			
			rs = stmt.executeQuery();

			while (rs.next()){
				country_name = rs.getString("country_name");
				if (country_name==null){
					country_name = "Unified Team";
				}				
				for (int i=0;i<medals.size();i++){
					if (medals.get(i).get("country_name").toString().equals(country_name)){
						medals.get(i).put("silver",rs.getString("num_medals"));						
					}
				}	
			}
			query = "select country_name, medal, count(*) as num_medals "
					+ "from participates join member on(athlete_id=member_id) "
					+ "join country using(country_code) "
					+ "where medal is not null and medal = 'B' "
					+ "group by country_name, medal "
					+ "order by (num_medals) desc";
			stmt = conn.prepareStatement(query);			
			rs = stmt.executeQuery();

			while (rs.next()){
				country_name = rs.getString("country_name");
				if (country_name==null){
					country_name = "Unified Team";
				}				
				for (int i=0;i<medals.size();i++){
					if (medals.get(i).get("country_name").toString().equals(country_name)){
						medals.get(i).put("bronze",rs.getString("num_medals"));						
					}
				}	
			}


			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Error getting medals tally " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
		}
		return medals;
	}
	ArrayList<HashMap<String, Object>> getMemberBookings(String memberID) throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> bookings = new ArrayList<HashMap<String, Object>>();

		String query = "select journey_id, vehicle_code, "
				+ "P1.place_name as origin, P2.place_name as destination, "
				+ "depart_time, arrive_time "
				+ "from booking join journey using(journey_id)"
				+ "join place P1 on(from_place = P1.place_id) "
				+ "join place P2 on(to_place = P2.place_id)"
				+ "where booked_for = ? "
				+ "order by depart_time desc";
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
				booking.put("origin_name", rs.getString("origin"));
				booking.put("dest_name", rs.getString("destination"));
				booking.put("when_departs", rs.getDate("depart_time"));
				booking.put("when_arrives", rs.getDate("arrive_time"));

				bookings.add(booking);
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Error finding member bookingsk " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
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
		HashMap<String,Object> details = new HashMap<String,Object>();
		String query = "select P1.place_name as depart_from, P2.place_name as arrive_to,"
				+ "journey_id,vehicle_code,depart_time,arrive_time,nbooked, capacity"
				+ " from Journey join Place P1 on(from_place = P1.place_id)"
				+" join Place P2 on(to_place = P2.place_id) join Vehicle using(vehicle_code) "
				+ " where journey_id = ?";
		PreparedStatement stmt = null;
		Connection conn = null;
		try{
			conn = getConnection();
			stmt = conn.prepareStatement(query);
			stmt.setInt(1,journeyId);
			System.out.println("looking for journey");
			ResultSet rs = stmt.executeQuery();
			System.out.println("found journey");
			int nr = 0;	

			while (rs.next()){	
				details.put("journey_id", Integer.valueOf(journeyId));
				details.put("vehicle_code",rs.getString("vehicle_code"));
				details.put("origin_name",rs.getString("depart_from"));
				details.put("dest_name",rs.getString("arrive_to"));
				details.put("when_departs",rs.getTimestamp("depart_time"));
				details.put("when_arrives",rs.getTimestamp("arrive_time"));
				details.put("capacity",rs.getInt("capacity"));
				details.put("nbooked",rs.getInt("nbooked"));
			}	
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Get Journey Details " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
		}

		return details;
	}

	public HashMap<String, Object> getBookingDetails(String memberID, Integer journeyId) throws OlympicsDBException {
		HashMap<String, Object> booking = null;

		String query = "select M1.family_name ||', ' || M1.given_names as member_for, "
				+ "M2.family_name ||', '|| M2.given_names as member_by, "
				+ "P1.place_name as origin, P2.place_name as destination, depart_time, arrive_time, vehicle_code, "
				+ "when_booked,journey_id "
				+ "from member M1 join booking on (M1.member_id=booked_for) "
				+ "join member M2 on (M2.member_id=booked_by) "
				+ "join journey using(journey_id) "
				+ "join place P1 on(from_place=P1.place_id) "
				+ "join place P2 on (to_place=P2.place_id)"
				+ "where journey_id = ? and booked_for = ?";
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
				booking.put("vehicle", rs.getString("vehicle_code"));
				booking.put("vehicle_code", rs.getString("vehicle_code"));
				booking.put("when_departs", rs.getDate("depart_time"));
				booking.put("dest_name", rs.getString("destination"));
				booking.put("origin_name", rs.getString("origin"));
				booking.put("bookedby_name", rs.getString("member_by"));
				booking.put("bookedfor_name", rs.getString("member_for"));
				booking.put("when_booked", rs.getDate("WHEN_BOOKED"));
				booking.put("when_arrives", rs.getDate("arrive_time"));
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			throw new OlympicsDBException("Error booking details " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
		}
		return booking;
	}
	public ArrayList<HashMap<String, Object>> getSports() throws OlympicsDBException {
		ArrayList<HashMap<String, Object>> sports = new ArrayList<HashMap<String, Object>>();
		String query = "SELECT SPORT.SPORT_ID, SPORT.SPORT_NAME, "
				+ "SPORT.DISCIPLINE FROM SPORT ORDER BY SPORT_NAME ASC";
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
		} catch (Exception e) {
			throw new OlympicsDBException("Error getting sports " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
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

	public HashMap<String,Object> makeBooking(String byStaff, String forMember,String Vehicle, Date departs) throws OlympicsDBException {
		HashMap<String,Object> booking = null;
		booking = new HashMap<String,Object>();

		PreparedStatement stmt1  = null;
		PreparedStatement stmt2  = null;
		PreparedStatement stmt3  = null;
		PreparedStatement stmt4  = null;
		PreparedStatement stmt5  = null;
		String query = "select * from staff where member_id = ?";
		boolean staffExists = false;
		boolean availability = false;
		Connection conn = null;
		try
		{
			conn = getConnection();
			conn.setAutoCommit(false);
			stmt1  = conn.prepareStatement(query);
			stmt1.setString(1,byStaff);
			ResultSet rs = stmt1.executeQuery(); 			
			staffExists = rs.next();				
			rs.close();

			if (!staffExists){
				conn.rollback();
				booking = null;
				System.out.println("Staff ID doesnt exist");
			}else{
				query = "select capacity, nbooked, journey_id, vehicle_code "
						+ "from Journey join vehicle using(vehicle_code) "
						+ "where vehicle_code  = ? and depart_time = ?";
				int journeyId = 0;
				int nbooked = 0;
				String date1 = "";
				SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy hh:mm:ss a");
				date1 += sdf.format(departs);
				stmt2  = conn.prepareStatement(query);
				stmt2.setString(1,Vehicle);
				stmt2.setString(2,date1);
				rs = stmt2.executeQuery(); 
				if (rs.next()){
					if (rs.getInt("nbooked")<rs.getInt("capacity")){
						availability = true;
					}
					nbooked = rs.getInt("nbooked");
					journeyId = rs.getInt("journey_id");
				}
				rs.close();
				if (!availability){
					conn.rollback();
					booking = null;
					System.out.println("Vehicle has no capacity");
				} else {
					stmt3 = conn.prepareStatement(query);				
					query = "insert into booking values(?,?,current_timestamp,?)";
					stmt3  = conn.prepareStatement(query);
					stmt3.setString(1,forMember);
					stmt3.setString(2,byStaff);
					stmt3.setInt(3,journeyId);
					int linesInserted1 = stmt3.executeUpdate();
					stmt3.close();
					query = "update journey set nbooked = ? where journey_id = ? and depart_time = ?";
					stmt4  = conn.prepareStatement(query);
					stmt4.setInt(1,nbooked+1);
					stmt4.setInt(2,journeyId);
					stmt4.setString(3,date1);
					int linesInserted2 = stmt4.executeUpdate();
					stmt4.close();
					System.out.println(linesInserted1);
					System.out.println(linesInserted2);
					if (linesInserted1==1 && linesInserted2==1){
						conn.commit();
						System.err.println("Success");
						query = "select M1.family_name || ', ' || M1.given_names as bookedfor_name, "
								+ "P1.place_name as origin_name, P2.place_name as dest_name, "
								+ "M2.family_name || ', ' || M2.given_names as bookedby_name,"
								+ "depart_time as when_departs, arrive_time as when_arrives,"
								+ "journey_id, when_booked,vehicle_code "
								+ "from Booking join Journey using (journey_id) "
								+ "join Place P1 on (from_place = P1.place_id) "
								+ "join Place P2 on (to_place = P2.place_id) "
								+ "join Member M1 on (booked_for = M1.member_id) "
								+ "join Member M2 on (booked_by = M2.member_id) "
								+ "where booked_for = ?  and booked_by = ? and vehicle_code = ? and depart_time = ?";
						System.err.println("Success");
						stmt5  = conn.prepareStatement(query);
						stmt5.setString(1,forMember);
						stmt5.setString(2,byStaff);
						stmt5.setString(3,Vehicle);
						stmt5.setString(4,date1);
						rs = stmt5.executeQuery();
						while (rs.next()){							
							booking.put("journey_id", rs.getInt("journey_id"));							
							//booking.put("vehicle_code",rs.getString("vehicle_code"));				
							booking.put("vehicle",rs.getString("vehicle_code"));							
							booking.put("bookedfor_name",rs.getString("bookedfor_name"));							
							booking.put("bookedby_name",rs.getString("bookedby_name"));							
							booking.put("dest_name",rs.getString("dest_name"));														
							booking.put("origin_name",rs.getString("origin_name"));							
							booking.put("when_departs",rs.getDate("when_departs"));							
							booking.put("when_arrives",rs.getDate("when_arrives"));							
							booking.put("when_booked", rs.getDate("WHEN_BOOKED"));
						}
						stmt1.close();
						stmt2.close();
						//stmt3.close();
						//stmt4.close();
						stmt5.close();
						rs.close();
						conn.close();
					} else {
						System.err.println("Insert unsuccessful");
						conn.rollback();
						booking = null;
					}
				}
			}
		}
		catch (Exception e) {
			throw new OlympicsDBException("Error getting sports " + e);
		} finally {
			if (conn!=null) reallyClose(conn);
		}
		return booking;
	}



}
