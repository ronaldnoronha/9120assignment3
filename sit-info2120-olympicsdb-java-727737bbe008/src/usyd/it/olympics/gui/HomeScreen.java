package usyd.it.olympics.gui;

import java.awt.BorderLayout;
import java.util.HashMap;

import javax.swing.JTextArea;
import usyd.it.olympics.OlympicsDBClient;

/**
 * Very simple home screen: Nothing fancy, just prints player details in a text area.
 * @author Bryn
 */
public class HomeScreen extends GuiScreen {

    private final JTextArea description;

    public HomeScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new BorderLayout(0, 0));

        description = new JTextArea();
        description.setEditable(false);
        panel_.add(description);
    }


	public void showMemberDetails(HashMap<String, Object> details) {
		// FIXME: Print the provided data
        String message = "Hello";
        message = message.concat("\nYou are an: " + "Athlete");
        message = message.concat("\nYou are from: " + "Australia");
        message = message.concat("\nYour live at: " + "SIT");
        message = message.concat("\nYour medal tally is:");
        message = message.concat("\n\tGold: 5");
        message = message.concat("\n\tSilver: 4");
        message = message.concat("\n\tBronze: 1");
        message = message.concat("\nYou have made 20 bookings");
        description.setText(message);		
	}
}
