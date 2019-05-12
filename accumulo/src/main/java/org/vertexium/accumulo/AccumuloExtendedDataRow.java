package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.ElementIterator;
import org.vertexium.accumulo.iterator.model.KeyBase;
import org.vertexium.accumulo.iterator.util.ArrayUtils;

import java.util.*;

public class AccumuloExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId rowId;
    private final Set<Property> properties;
    private final Set<String> additionalVisibilities;

    public AccumuloExtendedDataRow(
        ExtendedDataRowId rowId,
        Set<Property> properties,
        FetchHints fetchHints,
        Set<String> additionalVisibilities
    ) {
        super(fetchHints);
        this.rowId = rowId;
        this.properties = properties;
        this.additionalVisibilities = additionalVisibilities;
    }

    public static AccumuloExtendedDataRow create(
        ExtendedDataRowId rowId,
        SortedMap<Key, Value> row,
        FetchHints fetchHints,
        VertexiumSerializer vertexiumSerializer
    ) {
        Set<Property> properties = new HashSet<>();
        Set<String> additionalVisibilities = new HashSet<>();
        List<Map.Entry<Key, Value>> entries = new ArrayList<>(row.entrySet());
        entries.sort(Comparator.comparingLong(o -> o.getKey().getTimestamp()));
        for (Map.Entry<Key, Value> rowEntry : entries) {
            Text columnFamily = rowEntry.getKey().getColumnFamily();
            if (columnFamily.equals(AccumuloElement.CF_ADDITIONAL_VISIBILITY)) {
                String additionalVisibility = rowEntry.getKey().getColumnQualifier().toString();
                if (ArrayUtils.startsWith(rowEntry.getValue().get(), ElementIterator.ADDITIONAL_VISIBILITY_VALUE_DELETED.get())) {
                    additionalVisibilities.remove(additionalVisibility);
                } else {
                    additionalVisibilities.add(additionalVisibility);
                }
            } else if (columnFamily.equals(AccumuloElement.CF_EXTENDED_DATA)) {
                String[] columnQualifierParts = KeyBase.splitOnValueSeparator(rowEntry.getKey().getColumnQualifier().toString());
                if (columnQualifierParts.length != 1 && columnQualifierParts.length != 2) {
                    throw new VertexiumException("Invalid column qualifier for extended data row: " + rowId + " (expected 1 or 2 parts, found " + columnQualifierParts.length + ")");
                }
                String propertyName = columnQualifierParts[0];
                String propertyKey = columnQualifierParts.length > 1 ? columnQualifierParts[1] : null;
                Object propertyValue = vertexiumSerializer.bytesToObject(rowId, rowEntry.getValue().get());
                long timestamp = rowEntry.getKey().getTimestamp();
                Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(rowEntry.getKey().getColumnVisibility());
                AccumuloExtendedDataRowProperty prop = new AccumuloExtendedDataRowProperty(
                    propertyName,
                    propertyKey,
                    propertyValue,
                    fetchHints,
                    timestamp,
                    visibility
                );
                properties.add(prop);
            } else {
                throw new VertexiumException("unhandled column family: " + columnFamily);
            }
        }

        return new AccumuloExtendedDataRow(rowId, properties, fetchHints, additionalVisibilities);
    }

    @Override
    public ExtendedDataRowId getId() {
        return rowId;
    }

    @Override
    public Iterable<Property> getProperties() {
        return properties;
    }

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        return ImmutableSet.copyOf(additionalVisibilities);
    }

    private static class AccumuloExtendedDataRowProperty extends Property {
        private final String propertyName;
        private final String propertyKey;
        private final Object propertyValue;
        private final FetchHints fetchHints;
        private final long timestamp;
        private final Visibility visibility;

        public AccumuloExtendedDataRowProperty(
            String propertyName,
            String propertyKey,
            Object propertyValue,
            FetchHints fetchHints,
            long timestamp,
            Visibility visibility
        ) {
            this.propertyName = propertyName;
            this.propertyKey = propertyKey;
            this.propertyValue = propertyValue;
            this.fetchHints = fetchHints;
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
            return Metadata.create(fetchHints);
        }

        @Override
        public FetchHints getFetchHints() {
            return fetchHints;
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
