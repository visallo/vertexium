package org.vertexium.accumulo;

import org.vertexium.Visibility;
import org.vertexium.mutation.ExtendedDataMutation;

import java.util.HashSet;
import java.util.Set;

class TableNameVisibilityPair {
    private final Visibility visibility;
    private final String tableName;

    private TableNameVisibilityPair(Visibility visibility, String tableName) {
        this.visibility = visibility;
        this.tableName = tableName;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    public String getTableName() {
        return tableName;
    }

    public static Set<TableNameVisibilityPair> getUniquePairs(Iterable<ExtendedDataMutation> extendedDatas) {
        Set<TableNameVisibilityPair> results = new HashSet<>();
        for (ExtendedDataMutation extendedData : extendedDatas) {
            results.add(new TableNameVisibilityPair(extendedData.getVisibility(), extendedData.getTableName()));
        }
        return results;
    }

    @Override
    public String toString() {
        return "TableNameVisibilityPair{" +
            "visibility=" + visibility +
            ", tableName='" + tableName + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableNameVisibilityPair that = (TableNameVisibilityPair) o;

        if (!visibility.equals(that.visibility)) {
            return false;
        }
        return tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        int result = visibility.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }
}
