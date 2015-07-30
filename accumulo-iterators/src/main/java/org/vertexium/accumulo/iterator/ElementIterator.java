package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.iterators.user.RowEncodingIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.*;

import java.io.IOException;
import java.util.*;

public abstract class ElementIterator<T extends ElementData> extends RowEncodingIterator {
    public static final String CF_PROPERTY_STRING = "PROP";
    public static final Text CF_PROPERTY = new Text(CF_PROPERTY_STRING);
    public static final String CF_PROPERTY_HIDDEN_STRING = "PROPH";
    public static final Text CF_PROPERTY_HIDDEN = new Text(CF_PROPERTY_HIDDEN_STRING);
    public static final String CF_PROPERTY_SOFT_DELETE_STRING = "PROPD";
    public static final Text CF_PROPERTY_SOFT_DELETE = new Text(CF_PROPERTY_SOFT_DELETE_STRING);
    public static final String CF_PROPERTY_METADATA_STRING = "PROPMETA";
    public static final Text CF_PROPERTY_METADATA = new Text(CF_PROPERTY_METADATA_STRING);
    public static final String CF_HIDDEN_STRING = "H";
    public static final Text CF_HIDDEN = new Text(CF_HIDDEN_STRING);
    public static final Text CQ_HIDDEN = new Text("H");
    public static final String CF_SOFT_DELETE_STRING = "D";
    public static final Text CF_SOFT_DELETE = new Text(CF_SOFT_DELETE_STRING);
    public static final Text CQ_SOFT_DELETE = new Text("D");
    public static final Value HIDDEN_VALUE = new Value("".getBytes());
    public static final Value HIDDEN_VALUE_DELETED = new Value("X".getBytes());
    public static final Value SOFT_DELETE_VALUE = new Value("".getBytes());
    public static final String DELETE_ROW_COLUMN_FAMILY_STRING = "";
    public static final Text DELETE_ROW_COLUMN_FAMILY = new Text(DELETE_ROW_COLUMN_FAMILY_STRING);
    public static final String DELETE_ROW_COLUMN_QUALIFIER_STRING = "";
    public static final Text DELETE_ROW_COLUMN_QUALIFIER = new Text(DELETE_ROW_COLUMN_QUALIFIER_STRING);
    public static final String METADATA_COLUMN_FAMILY_STRING = "";
    public static final Text METADATA_COLUMN_FAMILY = new Text(METADATA_COLUMN_FAMILY_STRING);
    public static final String METADATA_COLUMN_QUALIFIER_STRING = "";
    public static final Text METADATA_COLUMN_QUALIFIER = new Text(METADATA_COLUMN_QUALIFIER_STRING);
    private static final String SETTING_FETCH_HINTS = "fetchHints";
    private EnumSet<FetchHint> fetchHints;
    private T elementData;
    private static final Map<Text, PropertyMetadataColumnQualifier> stringToPropertyMetadataColumnQualifierCache = new HashMap<>();

    public ElementIterator(SortedKeyValueIterator<Key, Value> source, EnumSet<FetchHint> fetchHints) {
        this.sourceIter = source;
        this.fetchHints = fetchHints;
        this.elementData = createElementData();
    }

    @Override
    public SortedMap<Key, Value> rowDecoder(Key rowKey, Value rowValue) throws IOException {
        throw new IOException("Not implemented");
    }

    @Override
    protected final boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
        return populateElementData(keys, values);
    }

    protected boolean populateElementData(List<Key> keys, List<Value> values) {
        this.elementData.clear();

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            Value value = values.get(i);
            if (!processKeyValue(key, value)) {
                return false;
            }
        }

        if (this.elementData.visibility == null) {
            return false;
        }

        if (this.elementData.softDeleteTimestamp >= this.elementData.timestamp) {
            return false;
        }

        return true;
    }

    @Override
    public final Value rowEncoder(List<Key> keys, List<Value> values) throws IOException {
        return elementData.encode(fetchHints);
    }

    private boolean processKeyValue(Key key, Value value) {
        if (this.elementData.id == null) {
            this.elementData.id = key.getRow();
        }

        Text columnFamily = key.getColumnFamily();

        if (CF_PROPERTY_METADATA.equals(columnFamily)) {
            extractPropertyMetadata(key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp(), value);
            return true;
        }

        if (CF_PROPERTY.equals(columnFamily)) {
            extractPropertyData(key, value);
            return true;
        }

        if (getVisibilitySignal().equals(columnFamily)) {
            elementData.visibility = key.getColumnVisibility();
            elementData.timestamp = key.getTimestamp();
            processSignalColumn(key.getColumnQualifier());
            return true;
        }

        if (processColumn(key, value, columnFamily, key.getColumnQualifier())) {
            return true;
        }

        if (DELETE_ROW_COLUMN_FAMILY.equals(columnFamily)
                && DELETE_ROW_COLUMN_QUALIFIER.equals(key.getColumnQualifier())
                && RowDeletingIterator.DELETE_ROW_VALUE.equals(value)) {
            return false;
        }

        if (CF_SOFT_DELETE.equals(columnFamily)
                && CQ_SOFT_DELETE.equals(key.getColumnQualifier())
                && SOFT_DELETE_VALUE.equals(value)) {
            elementData.softDeleteTimestamp = key.getTimestamp();
            return true;
        }

        if (CF_PROPERTY_SOFT_DELETE.equals(columnFamily)) {
            extractPropertySoftDelete(key.getColumnQualifier(), key.getTimestamp(), key.getColumnVisibility());
            return true;
        }

        if (CF_HIDDEN.equals(columnFamily)) {
            if (fetchHints.contains(FetchHint.INCLUDE_HIDDEN)) {
                this.elementData.hiddenVisibilities.add(key.getColumnVisibility());
                return true;
            } else {
                return false;
            }
        }

        if (CF_PROPERTY_HIDDEN.equals(columnFamily)) {
            extractPropertyHidden(key.getColumnQualifier(), key.getColumnVisibility(), value);
            return true;
        }

        return true;
    }

    protected abstract boolean processColumn(Key key, Value value, Text columnFamily, Text columnQualifier);

    protected void processSignalColumn(Text columnQualifier) {

    }

    public T getElementData() {
        return elementData;
    }

    protected abstract Text getVisibilitySignal();

    private void extractPropertySoftDelete(Text columnQualifier, long timestamp, Text columnVisibility) {
        PropertyColumnQualifier propertyColumnQualifier = new PropertyColumnQualifier(columnQualifier);
        SoftDeletedProperty softDeletedProperty = new SoftDeletedProperty(
                propertyColumnQualifier.getPropertyKey(),
                propertyColumnQualifier.getPropertyName(),
                timestamp,
                columnVisibility
        );
        this.elementData.softDeletedProperties.add(softDeletedProperty);
    }

    private void extractPropertyMetadata(Text columnQualifier, Text columnVisibility, long timestamp, Value value) {
        PropertyMetadataColumnQualifier propertyMetadataColumnQualifier = stringToPropertyMetadataColumnQualifierCache.get(columnQualifier);
        if (propertyMetadataColumnQualifier == null) {
            propertyMetadataColumnQualifier = new PropertyMetadataColumnQualifier(columnQualifier);
            stringToPropertyMetadataColumnQualifierCache.put(columnQualifier, propertyMetadataColumnQualifier);
        }
        String discriminator = propertyMetadataColumnQualifier.getPropertyDiscriminator(timestamp);
        PropertyMetadata propertyMetadata = elementData.propertyMetadata.get(discriminator);
        if (propertyMetadata == null) {
            propertyMetadata = new PropertyMetadata();
            elementData.propertyMetadata.put(discriminator, propertyMetadata);
        }
        propertyMetadata.add(propertyMetadataColumnQualifier.getMetadataKey(), columnVisibility.toString(), value.get());
    }

    private void extractPropertyHidden(Text columnQualifier, Text columnVisibility, Value value) {
        if (value.equals(HIDDEN_VALUE_DELETED)) {
            return;
        }
        PropertyHiddenColumnQualifier propertyHiddenColumnQualifier = new PropertyHiddenColumnQualifier(columnQualifier);
        HiddenProperty hiddenProperty = new HiddenProperty(
                propertyHiddenColumnQualifier.getPropertyKey(),
                propertyHiddenColumnQualifier.getPropertyName(),
                propertyHiddenColumnQualifier.getPropertyVisibilityString(),
                columnVisibility
        );
        this.elementData.hiddenProperties.add(hiddenProperty);
    }

    private void extractPropertyData(Key key, Value value) {
        PropertyColumnQualifier propertyColumnQualifier = new PropertyColumnQualifier(key.getColumnQualifier());
        String mapKey = propertyColumnQualifier.getDiscriminator(key.getColumnVisibility().toString(), key.getTimestamp());
        long timestamp = key.getTimestamp();
        this.elementData.propertyColumnQualifiers.put(mapKey, propertyColumnQualifier);
        this.elementData.propertyValues.put(mapKey, value.get());
        this.elementData.propertyVisibilities.put(mapKey, key.getColumnVisibility());
        this.elementData.propertyTimestamps.put(mapKey, timestamp);
    }

    @Override
    public abstract SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env);

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.get(SETTING_FETCH_HINTS) == null) {
            throw new IOException(SETTING_FETCH_HINTS + " is required");
        }
        fetchHints = FetchHint.parse(options.get(SETTING_FETCH_HINTS));
        elementData = createElementData();
    }

    protected abstract T createElementData();

    public static void setFetchHints(IteratorSetting iteratorSettings, EnumSet<FetchHint> fetchHints) {
        iteratorSettings.addOption(SETTING_FETCH_HINTS, FetchHint.toString(fetchHints));
    }

    public EnumSet<FetchHint> getFetchHints() {
        return fetchHints;
    }

    public T createElementDataFromRows(Iterator<Map.Entry<Key, Value>> rows) {
        List<Key> keys = new ArrayList<>();
        List<Value> values = new ArrayList<>();
        while (rows.hasNext()) {
            Map.Entry<Key, Value> row = rows.next();
            keys.add(row.getKey());
            values.add(row.getValue());
        }
        populateElementData(keys, values);
        return this.getElementData();
    }
}
