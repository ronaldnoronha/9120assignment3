package usyd.it.olympics.gui;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import usyd.it.olympics.OlympicsDBClient;
import usyd.it.olympics.data.Place;

public class BayFinderScreen extends GuiScreen {
    private final HashMapTupleTabelModel list = new HashMapTupleTabelModel(Place.idName, Place.attributeNames, Place.attributeClasses);
    private final ListSelectionModel listSelection;
    private final JTextField txtAddress;

    public BayFinderScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new BoxLayout(panel_, BoxLayout.Y_AXIS));

        JPanel choicePanel = new JPanel();
        choicePanel.setMaximumSize(new Dimension(32767, 23));
        panel_.add(choicePanel);
        choicePanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));

        JLabel lblAddress = new JLabel("Address");
        choicePanel.add(lblAddress);
        txtAddress = new JTextField();
        txtAddress.setText("address");
        choicePanel.add(txtAddress);
        txtAddress.setColumns(20);
        
        JButton btnUpdate = new JButton("Search");
        btnUpdate.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent arg0) {
                        client_.showMatchingBays(txtAddress.getText());
                }
        });
        choicePanel.add(btnUpdate);
        
        // Listing results
        JScrollPane listScrollPane = new JScrollPane();
        panel_.add(listScrollPane);

        JTable listTable = new JTable();
        listTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        listTable.setModel(list);
        listSelection = listTable.getSelectionModel();
        listScrollPane.setViewportView(listTable);


        JPanel selectionOptionsPanel = new JPanel();
        panel_.add(selectionOptionsPanel);
        // Include a button to allow a booking to be made for the currently selected row
        final JButton btnGetDetails = new JButton("Get Details");
        selectionOptionsPanel.add(btnGetDetails);

        btnGetDetails.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                int selectionIndex = listSelection.getMinSelectionIndex();
                if (selectionIndex >= 0) {
                    client_.getBayDetails(list.getTupleId(selectionIndex));
                }
            }
        });
        
        // Only allow row-specific buttons to work if a row has been selected
        btnGetDetails.setEnabled(false);

        listSelection.addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent e) {
                ListSelectionModel lsm = (ListSelectionModel) e.getSource();
                boolean isValid = !lsm.isSelectionEmpty();
                btnGetDetails.setEnabled(isValid);
            }
        });
    }

    public void showTuples(ArrayList<HashMap<String, Object>> newTuples) {
        list.update(newTuples);
    }

}
