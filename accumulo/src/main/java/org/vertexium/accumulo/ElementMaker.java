package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.vertexium.Authorizations;
import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.accumulo.keys.PropertyColumnQualifier;
import org.vertexium.accumulo.keys.PropertyHiddenColumnQualifier;
import org.vertexium.accumulo.keys.PropertyMetadataColumnQualifier;

import java.util.*;

public abstract class ElementMaker<T> {
    private final Iterator<Map.Entry<Key, Value>> row;
    private final Map<String, PropertyColumnQualifier> propertyColumnQualifiers = new HashMap<>();
    private final Map<String, byte[]> propertyValues = new HashMap<>();
    private final Map<String, Visibility> propertyVisibilities = new HashMap<>();
    private final Map<String, LazyPropertyMetadata> propertyMetadata = new HashMap<>();
    private final Map<String, Long> propertyTimestamps = new HashMap<>();
    private final Set<HiddenProperty> hiddenProperties = new HashSet<>();
    private final Set<SoftDeletedProperty> softDeletedProperties = new HashSet<>();
    private final Set<Visibility> hiddenVisibilities = new HashSet<>();
    private final AccumuloGraph graph;
    private final Authorizations authorizations;
    private String id;
    private Visibility visibility;
    private long elementSoftDeleteTimestamp;
    private static final Cache<Text, PropertyMetadataColumnQualifier> textToPropertyMetadataColumnQualifierCache = CacheBuilder
            .newCache(Text.class, PropertyMetadataColumnQualifier.class)
            .name(ElementMaker.class, "propertyMetadataColumnQualifierCache")
            .maxSize(10000)
            .build();

    public ElementMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        this.graph = graph;
        this.row = row;
        this.authorizations = authorizations;
    }

    public T make(boolean includeHidden) {
        while (row.hasNext()) {
            Map.Entry<Key, Value> col = row.next();

            if (this.id == null) {
                this.id = col.getKey().getRow().toString();
            }

            Text columnFamily = getColumnFamily(col.getKey());
            Text columnQualifier = getColumnQualifier(col.getKey().getColumnQualifier());
            long timestamp = col.getKey().getTimestamp();

            ColumnVisibility columnVisibility = AccumuloGraph.visibilityToAccumuloVisibility(col.getKey().getColumnVisibility().toString());
            Value value = col.getValue();

            if (columnFamily.equals(AccumuloGraph.DELETE_ROW_COLUMN_FAMILY)
                    && columnQualifier.equals(AccumuloGraph.DELETE_ROW_COLUMN_QUALIFIER)
                    && value.equals(RowDeletingIterator.DELETE_ROW_VALUE)) {
                return null;
            }

            if (columnFamily.equals(AccumuloElement.CF_SOFT_DELETE)
                    && columnQualifier.equals(AccumuloElement.CQ_SOFT_DELETE)
                    && value.equals(AccumuloElement.SOFT_DELETE_VALUE)) {
                elementSoftDeleteTimestamp = timestamp;
                continue;
            }

            if (columnFamily.equals(AccumuloElement.CF_HIDDEN)) {
                if (includeHidden) {
                    this.hiddenVisibilities.add(AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility));
                } else {
                    return null;
                }
            }

            if (columnFamily.equals(AccumuloElement.CF_PROPERTY_HIDDEN)) {
                extractPropertyHidden(columnQualifier, columnVisibility, value);
            }

            if (columnFamily.equals(AccumuloElement.CF_PROPERTY_SOFT_DELETE)) {
                extractPropertySoftDelete(columnQualifier, timestamp, columnVisibility);
            }

            if (AccumuloElement.CF_PROPERTY.compareTo(columnFamily) == 0) {
                extractPropertyData(col, columnVisibility);
                continue;
            }

            if (AccumuloElement.CF_PROPERTY_METADATA.compareTo(columnFamily) == 0) {
                extractPropertyMetadata(columnQualifier, columnVisibility, timestamp, value);
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

        if (this.elementSoftDeleteTimestamp >= getElementTimestamp()) {
            return null;
        }

        return makeElement(includeHidden);
    }

    protected abstract long getElementTimestamp();

    protected Text getColumnFamily(Key key) {
        return inflate(key.getColumnFamily());
    }

    protected Text getColumnQualifier(Text columnQualifier) {
        return inflate(columnQualifier);
    }

    private Text inflate(Text text) {
        return getGraph().getNameSubstitutionStrategy().inflate(text);
    }

    protected abstract void processColumn(Key key, Value value);

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
            PropertyColumnQualifier propertyColumnQualifier = propertyColumnQualifiers.get(key);
            String propertyKey = propertyColumnQualifier.getPropertyKey();
            String propertyName = propertyColumnQualifier.getPropertyName();
            byte[] propertyValue = propertyValueEntry.getValue();
            Visibility propertyVisibility = propertyVisibilities.get(key);
            long propertyTimestamp = propertyTimestamps.get(key);
            Set<Visibility> propertyHiddenVisibilities = getPropertyHiddenVisibilities(propertyKey, propertyName, propertyVisibility);
            if (!includeHidden && isHidden(propertyKey, propertyName, propertyVisibility)) {
                continue;
            }
            if (isPropertyDeleted(propertyKey, propertyName, propertyTimestamp, propertyVisibility)) {
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

    private boolean isHidden(String propertyKey, String propertyName, Visibility propertyVisibility) {
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return true;
            }
        }
        return false;
    }

    private boolean isPropertyDeleted(String propertyKey, String propertyName, long propertyTimestamp, Visibility propertyVisibility) {
        for (SoftDeletedProperty softDeletedProperty : softDeletedProperties) {
            if (softDeletedProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return softDeletedProperty.getTimestamp() >= propertyTimestamp;
            }
        }
        return false;
    }

    private void extractPropertyHidden(Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
        if (value.equals(AccumuloElement.HIDDEN_VALUE_DELETED)) {
            return;
        }
        PropertyHiddenColumnQualifier propertyHiddenColumnQualifier = new PropertyHiddenColumnQualifier(columnQualifier, getGraph().getNameSubstitutionStrategy());
        HiddenProperty hiddenProperty = new HiddenProperty(
                propertyHiddenColumnQualifier.getPropertyKey(),
                propertyHiddenColumnQualifier.getPropertyName(),
                propertyHiddenColumnQualifier.getPropertyVisibilityString(),
                AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility)
        );
        this.hiddenProperties.add(hiddenProperty);
    }

    private void extractPropertySoftDelete(Text columnQualifier, long timestamp, ColumnVisibility columnVisibility) {
        PropertyColumnQualifier propertyColumnQualifier = new PropertyColumnQualifier(columnQualifier, getGraph().getNameSubstitutionStrategy());
        SoftDeletedProperty softDeletedProperty = new SoftDeletedProperty(
                propertyColumnQualifier.getPropertyKey(),
                propertyColumnQualifier.getPropertyName(),
                timestamp,
                AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility)
        );
        this.softDeletedProperties.add(softDeletedProperty);
    }

    private void extractPropertyMetadata(Text columnQualifier, ColumnVisibility columnVisibility, long timestamp, Value value) {
        Visibility metadataVisibility = AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility);
        PropertyMetadataColumnQualifier propertyMetadataColumnQualifier = textToPropertyMetadataColumnQualifierCache.peek(columnQualifier);
        if (propertyMetadataColumnQualifier == null) {
            propertyMetadataColumnQualifier = new PropertyMetadataColumnQualifier(columnQualifier, getGraph().getNameSubstitutionStrategy());
            textToPropertyMetadataColumnQualifierCache.put(columnQualifier, propertyMetadataColumnQualifier);
        }
        String discriminator = propertyMetadataColumnQualifier.getPropertyDiscriminator(timestamp);
        LazyPropertyMetadata lazyPropertyMetadata = getOrCreatePropertyMetadata(discriminator);
        lazyPropertyMetadata.add(propertyMetadataColumnQualifier.getMetadataKey(), metadataVisibility, value.get());
    }

    private LazyPropertyMetadata getOrCreatePropertyMetadata(String discriminator) {
        LazyPropertyMetadata lazyPropertyMetadata = propertyMetadata.get(discriminator);
        if (lazyPropertyMetadata == null) {
            lazyPropertyMetadata = new LazyPropertyMetadata();
            propertyMetadata.put(discriminator, lazyPropertyMetadata);
        }
        return lazyPropertyMetadata;
    }

    private void extractPropertyData(Map.Entry<Key, Value> column, ColumnVisibility columnVisibility) {
        Text columnQualifier = getColumnQualifier(column.getKey().getColumnQualifier());
        Value value = column.getValue();
        Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility);
        PropertyColumnQualifier propertyColumnQualifier = new PropertyColumnQualifier(columnQualifier, getGraph().getNameSubstitutionStrategy());
        String mapKey = propertyColumnQualifier.getDiscriminator(visibility, column.getKey().getTimestamp());
        long timestamp = column.getKey().getTimestamp();
        propertyColumnQualifiers.put(mapKey, propertyColumnQualifier);
        propertyValues.put(mapKey, value.get());
        propertyVisibilities.put(mapKey, visibility);
        propertyTimestamps.put(mapKey, timestamp);
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    private static class SoftDeletedProperty {
        private final String propertyKey;
        private final String propertyName;
        private final long timestamp;
        private final Visibility visibility;

        public SoftDeletedProperty(String propertyKey, String propertyName, long timestamp, Visibility visibility) {
            this.propertyKey = propertyKey;
            this.propertyName = propertyName;
            this.timestamp = timestamp;
            this.visibility = visibility;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SoftDeletedProperty that = (SoftDeletedProperty) o;

            if (propertyKey != null ? !propertyKey.equals(that.propertyKey) : that.propertyKey != null) {
                return false;
            }
            if (propertyName != null ? !propertyName.equals(that.propertyName) : that.propertyName != null) {
                return false;
            }
            if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = propertyKey != null ? propertyKey.hashCode() : 0;
            result = 31 * result + (propertyName != null ? propertyName.hashCode() : 0);
            result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
            return result;
        }

        public boolean matches(String propertyKey, String propertyName, Visibility visibility) {
            return propertyKey.equals(this.propertyKey)
                    && propertyName.equals(this.propertyName)
                    && visibility.equals(this.visibility);
        }
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
