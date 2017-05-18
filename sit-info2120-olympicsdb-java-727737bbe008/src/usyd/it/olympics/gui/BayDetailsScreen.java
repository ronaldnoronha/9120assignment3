package usyd.it.olympics.gui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JTextArea;
import usyd.it.olympics.OlympicsDBClient;
import usyd.it.olympics.data.PlaceDetails;

/**
 * Very simple bay details screen, cloned from HomeScreen
 * @author Bryn
 */
public class BayDetailsScreen extends GuiScreen {

    private final JTextArea description;
    private final JButton btnMakeBooking;
    private Integer bayId;
    public BayDetailsScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new BoxLayout(panel_, BoxLayout.Y_AXIS));

        description = new JTextArea();
        description.setEditable(false);
        panel_.add(description);
        
        // Extra panel to include buttons
        // Include a button to allow a booking to be made for the current bay
        btnMakeBooking = new JButton("Make Booking");
        panel_.add(btnMakeBooking);
        btnMakeBooking.setEnabled(false);
        btnMakeBooking.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                client_.startBooking(bayId);
            }
        });   
    }

    public void showBayDetails(HashMap<String, Object> bay) {
        bayId = (Integer) bay.get(PlaceDetails.BAYID);
        btnMakeBooking.setEnabled(true);
        String s = "Name: " + bay.get(PlaceDetails.SITE)
                + "\nAddress: " + bay.get(PlaceDetails.HOUSENUM) + " " + bay.get(PlaceDetails.STREET)
                + ", " + bay.get(PlaceDetails.CITY)
                + "\nGPS co-ords: (" + bay.get(PlaceDetails.GPSLONG) + "," + bay.get(PlaceDetails.GPSLAT) + ")";

//        int[] hours = (int[]) bay.get(BayDetails.TODAYHOURS);
//        if (hours.length<1) {
//            s = s.concat("\nThere is no availability today");
//        } else {
//            String hrsText = "\nBay is available at the following hours";
//            for(int hour : hours) {
//                hrsText = hrsText.concat("\n" + hour + ":00");
//            }
//            s= s.concat(hrsText);
//        }
        description.setText(s);
    }
}
