package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.vertexium.*;

import java.util.*;

public class AccumuloExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId rowId;
    private final Map<String, Property> properties;

    public AccumuloExtendedDataRow(ExtendedDataRowId rowId, SortedMap<Key, Value> row, VertexiumSerializer vertexiumSerializer) {
        this.rowId = rowId;
        this.properties = rowToProperties(rowId, row, vertexiumSerializer);
    }

    private Map<String, Property> rowToProperties(
            ExtendedDataRowId rowId,
            SortedMap<Key, Value> row,
            VertexiumSerializer vertexiumSerializer
    ) {
        Map<String, Property> results = new HashMap<>();
        for (Map.Entry<Key, Value> rowEntry : row.entrySet()) {
            String propertyName = rowEntry.getKey().getColumnQualifier().toString();
            Object propertyValue = vertexiumSerializer.bytesToObject(rowEntry.getValue().get());
            long timestamp = rowEntry.getKey().getTimestamp();
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(rowEntry.getKey().getColumnVisibility());
            AccumuloExtendedDataRowProperty prop = new AccumuloExtendedDataRowProperty(
                    rowId.getTableName(),
                    propertyName,
                    propertyValue,
                    timestamp,
                    visibility
            );
            results.put(propertyName, prop);
        }
        return results;
    }

    @Override
    public ExtendedDataRowId getId() {
        return rowId;
    }

    @Override
    public Iterable<Property> getProperties() {
        return properties.values();
    }

    @Override
    public Property getProperty(String name) {
        return properties.get(name);
    }

    @Override
    public Iterable<String> getPropertyNames() {
        return properties.keySet();
    }

    @Override
    public Object getPropertyValue(String name) {
        Property property = getProperty(name);
        if (property == null) {
            return null;
        }
        return property.getValue();
    }

    private static class AccumuloExtendedDataRowProperty extends Property {
        private final String tableName;
        private final String propertyName;
        private final Object propertyValue;
        private final long timestamp;
        private final Visibility visibility;

        public AccumuloExtendedDataRowProperty(
                String tableName,
                String propertyName,
                Object propertyValue,
                long timestamp,
                Visibility visibility
        ) {
            this.tableName = tableName;
            this.propertyName = propertyName;
            this.propertyValue = propertyValue;
            this.timestamp = timestamp;
            this.visibility = visibility;
        }

        @Override
        public String getKey() {
            return tableName;
        }

        @Override
        public String getName() {
            return propertyName;
        }

        @Override
        public Object getValue() {
            return propertyValue;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }

        @Override
        public Metadata getMetadata() {
            return new Metadata();
        }

        @Override
        public Iterable<Visibility> getHiddenVisibilities() {
            return new ArrayList<>();
        }

        @Override
        public boolean isHidden(Authorizations authorizations) {
            return false;
        }
    }
}
