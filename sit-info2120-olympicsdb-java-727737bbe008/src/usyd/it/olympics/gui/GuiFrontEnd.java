package usyd.it.olympics.gui;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.BevelBorder;
import javax.swing.border.EtchedBorder;
import usyd.it.olympics.OlympicsDBClient;

/**
 * Main class for GUI, containing all screens and providing methods
 * to navigate between them.
 * @author Bryn
 */
public final class GuiFrontEnd {
    /// Parent reference for button ActionListeners
    private OlympicsDBClient client;
    
    //
    // Top-level GUI objects
    //
    private final JFrame window;
    private final JPanel mainPanel;
    private final CardLayout screenSelect;
    private final JLabel statusMsg;
    
    //
    // References to individual mainPanel screens
    //
    private final BayFinderScreen bayFinderScreen;
    private final HomeScreen mainMenuScreen;
    private final BayDetailsScreen bayDetailsScreen;
    private final ReportScreen generalReportScreen;
    private final BookingsCreationScreen bookingsCreationScreen;
    
    // Navigation buttons
    private final JPanel navBar;
    private final HashMap<String, JButton> buttons = new HashMap<String, JButton>();
    private final String optHome;
    private final String optFavourite;
    private final String optBrowseBays;
    private final String optHistory;
    private final String optLogOut;
    private final BookingHistoryScreen historyScreen;

    public GuiFrontEnd(OlympicsDBClient root) {
        client = root;
        window = new JFrame("Client GUI");
        window.setPreferredSize(new Dimension(800, 400));
        window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        // This will be the content pane for the window
        JPanel content = new JPanel();
        content.setLayout(new BorderLayout());


        // Navigation buttons at top
        navBar = new JPanel();
        navBar.setBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null));
        
        // Put navigation buttons in bar
        optHome = "Home";
        addMenuOption(optHome, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                client.showMemberDetails();
            }
        });

        optFavourite = "Favourite";
        addMenuOption(optFavourite, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                client.showFavouriteBay();
            }
        });
        
        optBrowseBays = "Browse Bays";
        addMenuOption(optBrowseBays, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                client.showBayAvailability();
            }
        });

        optHistory = "History";
        addMenuOption(optHistory,new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                client.showHistory();
            }
        });

        optLogOut = "Log Out";
        addMenuOption(optLogOut, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                client.logout();
            }
        });

        setNavButtons(false);
        content.add(navBar, BorderLayout.NORTH);

        // Status messages at bottom
        JPanel statusPanel = new JPanel();
        statusPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
        statusMsg = new JLabel("Status");
        statusPanel.setMinimumSize(new Dimension(100, 40));
        statusPanel.setPreferredSize(new Dimension(100, 40));
        statusPanel.setLayout(new GridLayout(1, 1, 0, 0));
        statusPanel.add(statusMsg);
        content.add(statusPanel, BorderLayout.SOUTH);

        // All main interface stuff goes into a central panel
        mainPanel = new JPanel();
        screenSelect = new CardLayout();
        mainPanel.setLayout(screenSelect);

        registerScreen(new LoginScreen(root), "loginScreen");
        registerScreen(mainMenuScreen = new HomeScreen(root), "mainMenuScreen");
        registerScreen(bayFinderScreen = new BayFinderScreen(root), "bayFinderScreen");
        registerScreen(bayDetailsScreen = new BayDetailsScreen(root), "bayDetailsScreen");
        registerScreen(generalReportScreen = new ReportScreen(root), "generalReportScreen");
        registerScreen(historyScreen = new BookingHistoryScreen(root), "historyScreen");
        registerScreen(bookingsCreationScreen = new BookingsCreationScreen(root), "bookingsCreationScreen");

        content.add(mainPanel, BorderLayout.CENTER);
        window.setContentPane(content);
        window.pack();
        window.setVisible(true);
    }

    // Control all navbar buttons together
    public void setNavButtons(boolean status) {
        for (JButton button : buttons.values()) {
            button.setEnabled(status);
        }
    }
    
    public void setNavButton(String name, boolean status) {
        buttons.get(name).setEnabled(status);
    }
    
    public void showLoginScreen() {
        screenSelect.show(mainPanel, "loginScreen");
        setNavButtons(false);
    }

    public void showMainMenuScreen() {
        screenSelect.show(mainPanel, "mainMenuScreen");
        setNavButtons(true);
        setNavButton(optHome, false);
    }

    public void showBayStatusScreen() {
        screenSelect.show(mainPanel, "bayStatusScreen");
        setNavButtons(true);
        setNavButton(optFavourite, false);
    }
        
    public void showBayFinderScreen() {
        screenSelect.show(mainPanel, "bayFinderScreen");
        setNavButtons(true);
        setNavButton(optBrowseBays, false);
    }
    
    public void showBayDetailsScreen() {
        screenSelect.show(mainPanel, "bayDetailsScreen");
        setNavButtons(true);
    }

    public void showReportScreen() {
        screenSelect.show(mainPanel, "generalReportScreen");
        setNavButtons(true);
    }
    
    public void showHistoryScreen() {
        screenSelect.show(mainPanel, "historyScreen");
        setNavButtons(true);
    }
    
    public void showBookingsCreationScreen() {
        screenSelect.show(mainPanel, "bookingsCreationScreen");
        setNavButtons(true);
    }

    public void setStatus(String msg) {
        statusMsg.setText(msg);
    }

    public HomeScreen getMainMenuScreen() {
        return mainMenuScreen;
    }
       
    public BayFinderScreen getBayFinderScreen() {
        return bayFinderScreen;
    }    
    
    public ReportScreen getReportScreen() {
        return generalReportScreen;
    }
    
    public BayDetailsScreen getBayDetailsScreen() {
        return bayDetailsScreen;
    }
    
    public BookingHistoryScreen getHistoryScreen() {
        return historyScreen;
    }
    
    public BookingsCreationScreen getBookingsCreationScreen() {
        return bookingsCreationScreen;
    }
        
    private void registerScreen(GuiScreen screen, String label) {
        mainPanel.add(screen.getPanel(), label);
    }

    private JButton addMenuOption(String label, ActionListener actionListener) {
        JButton button = new JButton(label);
        button.addActionListener(actionListener);
        navBar.add(button);
        buttons.put(label, button);
        return button;
    }

}
