package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.ArrayList;
import java.util.List;

public class JsonContentSchema {

    private TableSchema schema;

    public static class TableSchema {

    }

    private int numRows;
    private int numCols;

    private List<Tuple> tuples = new ArrayList<>();

    public static class Tuple {

    }

    private String[] columns;
    private double confidence;
    private int source;
    private int tableId;

    public int getNumRows() {
        return numRows;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public int getNumCols() {
        return numCols;
    }

    public void setNumCols(int numCols) {
        this.numCols = numCols;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public int getSource() {
        return source;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }
}