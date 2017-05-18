package usyd.it.olympics.data;

import java.util.Date;

/**
 *
 * @author Bryn Jeffries
 */
public class BookingTuple {
    public static final String idName = "booking_id";
	public static final String[] columnNames = { "Car", "Parking Bay", "From", "Till"};
	public static final Class<?>[] columnClasses = { String.class, String.class, Date.class, Date.class};
}
