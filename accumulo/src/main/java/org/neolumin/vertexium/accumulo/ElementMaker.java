package org.neolumin.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.VertexiumException;
import org.neolumin.vertexium.Visibility;

import java.util.*;

public abstract class ElementMaker<T> {
    private final Iterator<Map.Entry<Key, Value>> row;
    private final Map<String, String> propertyNames = new HashMap<>();
    private final Map<String, String> propertyColumnQualifier = new HashMap<>();
    private final Map<String, byte[]> propertyValues = new HashMap<>();
    private final Map<String, Visibility> propertyVisibilities = new HashMap<>();
    private final Map<String, LazyPropertyMetadata> propertyMetadata = new HashMap<>();
    private final Map<String, Long> propertyTimestamps = new HashMap<>();
    private final Set<HiddenProperty> hiddenProperties = new HashSet<>();
    private final Set<Visibility> hiddenVisibilities = new HashSet<>();
    private final AccumuloGraph graph;
    private final Authorizations authorizations;
    private String id;
    private Visibility visibility;

    public ElementMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        this.graph = graph;
        this.row = row;
        this.authorizations = authorizations;
    }

    public T make(boolean includeHidden) {
        while (row.hasNext()) {
            Map.Entry<Key, Value> col = row.next();

            if (this.id == null) {
                this.id = getIdFromRowKey(col.getKey().getRow().toString());
            }

            Text columnFamily = getColumnFamily(col.getKey());
            Text columnQualifier = getColumnQualifier(col.getKey());;
            ColumnVisibility columnVisibility = AccumuloGraph.visibilityToAccumuloVisibility(col.getKey().getColumnVisibility().toString());
            Value value = col.getValue();

            if (columnFamily.equals(AccumuloGraph.DELETE_ROW_COLUMN_FAMILY)
                    && columnQualifier.equals(AccumuloGraph.DELETE_ROW_COLUMN_QUALIFIER)
                    && value.equals(RowDeletingIterator.DELETE_ROW_VALUE)) {
                return null;
            }

            if (columnFamily.equals(AccumuloElement.CF_HIDDEN)) {
                if (includeHidden) {
                    this.hiddenVisibilities.add(AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility));
                } else {
                    return null;
                }
            }

            if (columnFamily.equals(AccumuloElement.CF_PROPERTY_HIDDEN)) {
                extractPropertyHidden(columnQualifier, columnVisibility);
            }

            if (AccumuloElement.CF_PROPERTY.compareTo(columnFamily) == 0) {
                extractPropertyData(col, columnVisibility);
                continue;
            }

            if (AccumuloElement.CF_PROPERTY_METADATA.compareTo(columnFamily) == 0) {
                extractPropertyMetadata(columnQualifier, columnVisibility, value);
                continue;
            }

            if (getVisibilitySignal().equals(columnFamily.toString())) {
                this.visibility = AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility);
            }

            processColumn(col.getKey(), col.getValue());
        }

        // If the ElementVisibilityRowFilter isn't installed this will catch stray rows
        if (this.visibility == null) {
            return null;
        }

        return makeElement(includeHidden);
    }

    protected Text getColumnFamily(Key key){
        return key.getColumnFamily();
    }

    protected Text getColumnQualifier(Key key){
        return key.getColumnQualifier();
    }

    protected abstract void processColumn(Key key, Value value);

    protected abstract String getIdFromRowKey(String rowKey);

    protected abstract String getVisibilitySignal();

    protected abstract T makeElement(boolean includeHidden);

    protected String getId() {
        return this.id;
    }

    protected Visibility getVisibility() {
        return this.visibility;
    }

    public AccumuloGraph getGraph() {
        return graph;
    }

    protected Set<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    protected List<Property> getProperties(boolean includeHidden) {
        List<Property> results = new ArrayList<>(propertyValues.size());
        for (Map.Entry<String, byte[]> propertyValueEntry : propertyValues.entrySet()) {
            String key = propertyValueEntry.getKey();
            String propertyKey = getPropertyKeyFromColumnQualifier(propertyColumnQualifier.get(key));
            String propertyName = propertyNames.get(key);
            byte[] propertyValue = propertyValueEntry.getValue();
            Visibility propertyVisibility = propertyVisibilities.get(key);
            long propertyTimestamp = propertyTimestamps.get(key);
            Set<Visibility> propertyHiddenVisibilities = getPropertyHiddenVisibilities(propertyKey, propertyName, propertyVisibility);
            if (!includeHidden && isHidden(propertyKey, propertyName, propertyVisibility)) {
                continue;
            }
            LazyPropertyMetadata metadata = propertyMetadata.get(key);
            LazyMutableProperty property = new LazyMutableProperty(
                    getGraph(),
                    getGraph().getValueSerializer(),
                    propertyKey,
                    propertyName,
                    propertyValue,
                    metadata,
                    propertyHiddenVisibilities,
                    propertyVisibility,
                    propertyTimestamp
            );
            results.add(property);
        }
        return results;
    }

    private Set<Visibility> getPropertyHiddenVisibilities(String propertyKey, String propertyName, Visibility propertyVisibility) {
        Set<Visibility> hiddenVisibilities = null;
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                if (hiddenVisibilities == null) {
                    hiddenVisibilities = new HashSet<>();
                }
                hiddenVisibilities.add(hiddenProperty.getHiddenVisibility());
            }
        }
        return hiddenVisibilities;
    }

    private boolean isHidden(String propertyKey, String propertyName, Visibility visibility) {
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, visibility)) {
                return true;
            }
        }
        return false;
    }

    private void extractPropertyHidden(Text columnQualifier, ColumnVisibility columnVisibility) {
        String columnQualifierStr = columnQualifier.toString();
        int nameKeySep = columnQualifierStr.indexOf(ElementMutationBuilder.VALUE_SEPARATOR);
        if (nameKeySep < 0) {
            throw new VertexiumException("Invalid property hidden column qualifier");
        }
        int keyVisSep = columnQualifierStr.indexOf(ElementMutationBuilder.VALUE_SEPARATOR, nameKeySep + 1);
        if (nameKeySep < 0) {
            throw new VertexiumException("Invalid property hidden column qualifier");
        }

        String name = columnQualifierStr.substring(0, nameKeySep);
        String key = columnQualifierStr.substring(nameKeySep + 1, keyVisSep);
        String vis = columnQualifierStr.substring(keyVisSep + 1);

        this.hiddenProperties.add(new HiddenProperty(key, name, vis, AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility)));
    }

    private void extractPropertyMetadata(Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
        Visibility metadataVisibility = AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility);
        String columnQualifierString = columnQualifier.toString();
        int i = columnQualifierString.lastIndexOf(ElementMutationBuilder.VALUE_SEPARATOR);
        if (i < 0) {
            throw new VertexiumException("Invalid property metadata column qualifier: " + columnQualifierString);
        }
        String propertyKey = columnQualifierString.substring(0, i);
        String metadataKey = columnQualifierString.substring(i + 1);

        LazyPropertyMetadata lazyPropertyMetadata = getOrCreatePropertyMetadata(propertyKey);
        lazyPropertyMetadata.add(metadataKey, metadataVisibility, value.get());
    }

    private LazyPropertyMetadata getOrCreatePropertyMetadata(String propertyKey) {
        LazyPropertyMetadata lazyPropertyMetadata = propertyMetadata.get(propertyKey);
        if (lazyPropertyMetadata == null) {
            lazyPropertyMetadata = new LazyPropertyMetadata();
            propertyMetadata.put(propertyKey, lazyPropertyMetadata);
        }
        return lazyPropertyMetadata;
    }

    private void extractPropertyData(Map.Entry<Key, Value> column, ColumnVisibility columnVisibility) {
        Text columnQualifier = getColumnQualifier(column.getKey());
        Value value = column.getValue();
        Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility);
        String propertyName = getPropertyNameFromColumnQualifier(columnQualifier.toString());
        String key = propertyColumnQualifierToKey(columnQualifier, visibility);
        long timestamp = column.getKey().getTimestamp();
        propertyColumnQualifier.put(key, columnQualifier.toString());
        propertyNames.put(key, propertyName);
        propertyValues.put(key, value.get());
        propertyVisibilities.put(key, visibility);
        propertyTimestamps.put(key, timestamp);
    }

    private String propertyColumnQualifierToKey(Text columnQualifier, Visibility visibility) {
        return columnQualifier.toString() + ElementMutationBuilder.VALUE_SEPARATOR + visibility.toString();
    }

    private String getPropertyNameFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(ElementMutationBuilder.VALUE_SEPARATOR);
        if (i < 0) {
            throw new VertexiumException("Invalid property column qualifier");
        }
        return columnQualifier.substring(0, i);
    }

    private String getPropertyKeyFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(ElementMutationBuilder.VALUE_SEPARATOR);
        if (i < 0) {
            throw new VertexiumException("Invalid property column qualifier");
        }
        return columnQualifier.substring(i + 1);
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    private static class HiddenProperty {
        private final String key;
        private final String name;
        private final String visibility;
        private final Visibility hiddenVisibility;

        public HiddenProperty(String key, String name, String visibility, Visibility hiddenVisibility) {
            this.key = key;
            this.name = name;
            this.visibility = visibility;
            this.hiddenVisibility = hiddenVisibility;
        }

        public boolean matches(String propertyKey, String propertyName, Visibility visibility) {
            return propertyKey.equals(this.key)
                    && propertyName.equals(this.name)
                    && visibility.getVisibilityString().equals(this.visibility);
        }

        public Visibility getHiddenVisibility() {
            return hiddenVisibility;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HiddenProperty that = (HiddenProperty) o;

            if (key != null ? !key.equals(that.key) : that.key != null) {
                return false;
            }
            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
            if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "HiddenProperty{" +
                    "key='" + key + '\'' +
                    ", name='" + name + '\'' +
                    ", visibility='" + visibility + '\'' +
                    '}';
        }
    }
}
