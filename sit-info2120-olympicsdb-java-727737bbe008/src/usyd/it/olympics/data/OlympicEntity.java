package usyd.it.olympics.data;

import java.util.Date;
import java.util.HashMap;

/**
 * An event in the olympics and it's necessary attributes.
 * @author INFO2120
 */
public class OlympicEntity implements BasicEntity {
    String name_;
    Date starts_;
    String resultType_;
    String sportName_;
    String venueName_;

    public OlympicEntity(String name, Date starts, String resultType, String sportName, String venue) {
        name_ = name;
        starts_ = starts;
        resultType_ = resultType;
        sportName_ = sportName;
        venueName_ = venue;
    }

    /**
     * Returns the hash map of the event
     * @return - Hash map of the event
     */
    @Override
    public HashMap<String, Object> asHashMap(){
        HashMap<String, Object> eventHashMap = new HashMap<>();
        eventHashMap.put("name", name_);
        eventHashMap.put("start", starts_);
        eventHashMap.put("sport", sportName_);
        eventHashMap.put("venue", venueName_);

        return eventHashMap;
    }

    public String getName(){
        return name_;
    }

    public Date getStart(){
        return starts_;
    }

    public String getResultType(){
        return resultType_;
    }

    public String getSportName(){
        return sportName_;
    }

    public String getVenueName(){
        return venueName_;
    }
}
