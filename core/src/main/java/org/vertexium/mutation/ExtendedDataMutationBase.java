package org.vertexium.mutation;

import com.google.common.collect.Ordering;
import org.vertexium.Visibility;
import org.vertexium.util.KeyUtils;

public class ExtendedDataMutationBase<T extends ExtendedDataMutationBase> implements Comparable<T> {
    private final String tableName;
    private final String row;
    private final String columnName;
    private final String key;
    private final Visibility visibility;

    public ExtendedDataMutationBase(String tableName, String row, String columnName, String key, Visibility visibility) {
        KeyUtils.checkKey(tableName, "Invalid tableName");
        KeyUtils.checkKey(row, "Invalid row");
        KeyUtils.checkKey(columnName, "Invalid columnName");
        KeyUtils.checkKey(key, "Invalid key");
        this.tableName = tableName;
        this.row = row;
        this.columnName = columnName;
        this.key = key;
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

    public String getKey() {
        return key;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "tableName='" + tableName + '\'' +
            ", row='" + row + '\'' +
            ", columnName='" + columnName + '\'' +
            ", key='" + key + '\'' +
            ", visibility=" + visibility +
            '}';
    }

    @Override
    public int compareTo(T other) {
        int i = tableName.compareTo(other.getTableName());
        if (i != 0) {
            return i;
        }

        i = row.compareTo(other.getRow());
        if (i != 0) {
            return i;
        }

        i = columnName.compareTo(other.getColumnName());
        if (i != 0) {
            return i;
        }

        i = Ordering.natural().nullsFirst().compare(key, other.getKey());
        if (i != 0) {
            return i;
        }

        return 0;
    }
}
