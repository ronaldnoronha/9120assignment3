package usyd.it.olympics.data;

import java.util.HashMap;

/**
 * Class to hold details for a bay as used by BayFinderScreen
 * @author Bryn
 */
public class Place {
    public static final String idName = "bay_id";
	public static final String[] attributeNames = {"Site", "Number", "Address", "City"};
	public static final Class<?>[] attributeClasses = {String.class, String.class, String.class, String.class};
	private static final String PLACEID = "place_id";

    private final int bayId_;
    private final String site_;
    private final String houseNum_;
    private final String street_;
    private final String city_;

    /**
     * Create a new object to hold the details for a bay
     *
     */
    public Place(int bayId, String site, String houseNum, String street, String city) {
        bayId_ = bayId;
        site_ = site;
        houseNum_ = houseNum;
        street_ = street;
        city_ = city;
    }

    /**
     * @return the bayId_
     */
    public int getBayId() {
        return bayId_;
    }

    /**
     * @return the site_
     */
    public String getSite() {
        return site_;
    }

    /**
     * @return the houseNum_
     */
    public String getHouseNum() {
        return houseNum_;
    }

    /**
     * @return the street
     */
    public String getStreet() {
        return street_;
    }

    /**
     * @return the city_
     */
    public String getCity() {
        return city_;
    }

	public static Integer getPlaceId(HashMap<String, Object> place) {
		return (Integer) place.get(PLACEID);
	}

}
