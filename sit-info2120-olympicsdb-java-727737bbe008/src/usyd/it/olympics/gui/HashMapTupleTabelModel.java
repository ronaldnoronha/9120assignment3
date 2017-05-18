package usyd.it.olympics.gui;

import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.table.AbstractTableModel;

@SuppressWarnings("serial")
class HashMapTupleTabelModel extends AbstractTableModel {
        private ArrayList<HashMap<String, Object>> tuples;

        private final String idName;
        private final String [] columnNames;
        private final Class<?>[] columnClasses;

    public Class<?> getColumnClass(int columnIndex) {
                return columnClasses[columnIndex];
        }

    public HashMapTupleTabelModel(String tupleIdName, String [] tupleColumnNames, Class<?>[] tupleColumnClasses) {
    	idName = tupleIdName;
    	columnNames = tupleColumnNames;
    	columnClasses = tupleColumnClasses;
        tuples = new ArrayList<HashMap<String, Object>>();
        }

        /**
            * Update the table with newly supplied data
            * @param newCars New list of cars
            */
        public void update(ArrayList<HashMap<String, Object>> newtuples) {
                tuples = newtuples;
                super.fireTableDataChanged();
        }
        
        public Integer getTupleId(int row) {
            HashMap<String, Object> tuple = tuples.get(row);
			return tuple==null ? null : (Integer) tuple.get(idName);
        }

        /*
            * AbstractTableModel methods
            */
        @Override
        public String getColumnName(int col) {
                return columnNames[col];
        }

        @Override
        public int getColumnCount() {
                return columnNames.length;
        }

        @Override
        public int getRowCount() {
                return tuples.size();
        }

        @Override
        public Object getValueAt(int row, int col) {
            Object o=null;
            HashMap<String, Object> tuple = tuples.get(row);
            o = tuple!=null && col>=0 && col<columnNames.length ? tuple.get(columnNames[col]) : null;
            return o;
        }

}