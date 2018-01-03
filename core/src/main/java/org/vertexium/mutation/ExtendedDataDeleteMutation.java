package org.vertexium.mutation;

import org.vertexium.Visibility;

public class ExtendedDataDeleteMutation implements Comparable<ExtendedDataDeleteMutation> {
    private final String tableName;
    private final String row;
    private final String columnName;
    private final Visibility visibility;

    public ExtendedDataDeleteMutation(String tableName, String row, String columnName, Visibility visibility) {
        this.tableName = tableName;
        this.row = row;
        this.columnName = columnName;
        this.visibility = visibility;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRow() {
        return row;
    }

    public String getColumnName() {
        return columnName;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "ExtendedDataDeleteMutation{" +
                "tableName='" + tableName + '\'' +
                ", row='" + row + '\'' +
                ", columnName='" + columnName + '\'' +
                ", visibility=" + visibility +
                '}';
    }

    @Override
    public int compareTo(ExtendedDataDeleteMutation other) {
        int i = tableName.compareTo(other.tableName);
        if (i != 0) {
            return i;
        }

        i = row.compareTo(other.row);
        if (i != 0) {
            return i;
        }

        i = columnName.compareTo(other.columnName);
        if (i != 0) {
            return i;
        }

        return 0;
    }
}
