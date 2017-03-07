package org.vertexium.mutation;

import org.vertexium.Visibility;
import org.vertexium.util.IncreasingTime;

public class ExtendedDataMutation implements Comparable<ExtendedDataMutation> {
    private final String tableName;
    private final String row;
    private final String columnName;
    private final Object value;
    private final long timestamp;
    private final Visibility visibility;

    public ExtendedDataMutation(
            String tableName,
            String row,
            String columnName,
            Object value,
            Long timestamp,
            Visibility visibility
    ) {
        this.tableName = tableName;
        this.row = row;
        this.columnName = columnName;
        this.value = value;
        this.timestamp = timestamp == null ? IncreasingTime.currentTimeMillis() : timestamp;
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

    public Object getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "ExtendedDataMutation{" +
                "tableName='" + tableName + '\'' +
                ", row='" + row + '\'' +
                ", columnName='" + columnName + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ", visibility=" + visibility +
                '}';
    }

    @Override
    public int compareTo(ExtendedDataMutation other) {
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
