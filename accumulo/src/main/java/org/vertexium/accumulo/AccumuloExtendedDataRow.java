package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.model.KeyBase;

import java.util.*;

public class AccumuloExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId rowId;
    private final Set<Property> properties;

    public AccumuloExtendedDataRow(ExtendedDataRowId rowId, SortedMap<Key, Value> row, VertexiumSerializer vertexiumSerializer) {
        this.rowId = rowId;
        this.properties = rowToProperties(rowId, row, vertexiumSerializer);
    }

    private Set<Property> rowToProperties(
            ExtendedDataRowId rowId,
            SortedMap<Key, Value> row,
            VertexiumSerializer vertexiumSerializer
    ) {
        Set<Property> results = new HashSet<>();
        for (Map.Entry<Key, Value> rowEntry : row.entrySet()) {
            String[] columnQualifierParts = KeyBase.splitOnValueSeparator(rowEntry.getKey().getColumnQualifier().toString());
            if (columnQualifierParts.length != 1 && columnQualifierParts.length != 2) {
                throw new VertexiumException("Invalid column qualifier for extended data row: " + rowId + " (expected 1 or 2 parts, found " + columnQualifierParts.length + ")");
            }
            String propertyName = columnQualifierParts[0];
            String propertyKey = columnQualifierParts.length > 1 ? columnQualifierParts[1] : null;
            Object propertyValue = vertexiumSerializer.bytesToObject(rowEntry.getValue().get());
            long timestamp = rowEntry.getKey().getTimestamp();
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(rowEntry.getKey().getColumnVisibility());
            AccumuloExtendedDataRowProperty prop = new AccumuloExtendedDataRowProperty(
                    propertyName,
                    propertyKey,
                    propertyValue,
                    timestamp,
                    visibility
            );
            results.add(prop);
        }
        return results;
    }

    @Override
    public ExtendedDataRowId getId() {
        return rowId;
    }

    @Override
    public Iterable<Property> getProperties() {
        return properties;
    }

    private static class AccumuloExtendedDataRowProperty extends Property {
        private final String propertyName;
        private final String propertyKey;
        private final Object propertyValue;
        private final long timestamp;
        private final Visibility visibility;

        public AccumuloExtendedDataRowProperty(
                String propertyName,
                String propertyKey,
                Object propertyValue,
                long timestamp,
                Visibility visibility
        ) {
            this.propertyName = propertyName;
            this.propertyKey = propertyKey;
            this.propertyValue = propertyValue;
            this.timestamp = timestamp;
            this.visibility = visibility;
        }

        @Override
        public String getKey() {
            return propertyKey;
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
