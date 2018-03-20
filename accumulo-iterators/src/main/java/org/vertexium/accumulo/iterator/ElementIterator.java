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
import java.util.regex.Pattern;

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
    public static final String CF_EXTENDED_DATA_STRING = "EXTDATA";
    public static final Text CF_EXTENDED_DATA = new Text(CF_EXTENDED_DATA_STRING);
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
    private static final String SETTING_FETCH_HINTS_PREFIX = "fetchHints.";
    private static final String RECORD_SEPARATOR = "\u001f";
    private static final Pattern RECORD_SEPARATOR_PATTERN = Pattern.compile(Pattern.quote(RECORD_SEPARATOR));
    private IteratorFetchHints fetchHints;
    private T elementData;
    private static final Map<Text, PropertyMetadataColumnQualifier> stringToPropertyMetadataColumnQualifierCache = new HashMap<>();

    public ElementIterator(SortedKeyValueIterator<Key, Value> source, IteratorFetchHints fetchHints) {
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

        Text columnFamily = new Text();
        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            Value value = values.get(i);
            key.getColumnFamily(columnFamily); // avoid Text allocation by reusing columnFamily
            if (!processKeyValue(key, columnFamily, value)) {
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

    private boolean processKeyValue(Key key, Text columnFamily, Value value) {
        if (this.elementData.id == null) {
            this.elementData.id = key.getRow();
        }

        if (CF_PROPERTY_METADATA.equals(columnFamily)) {
            extractPropertyMetadata(key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp(), value);
            return true;
        }

        if (CF_PROPERTY.equals(columnFamily)) {
            extractPropertyData(key, value);
            return true;
        }

        if (CF_EXTENDED_DATA.equals(columnFamily)) {
            this.elementData.extendedTableNames.add(value.toString());
            return true;
        }

        if (getVisibilitySignal().equals(columnFamily) && key.getTimestamp() > elementData.timestamp) {
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
            if (fetchHints.isIncludeHidden()) {
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
        if (shouldIncludeMetadata(propertyMetadataColumnQualifier)) {
            String discriminator = propertyMetadataColumnQualifier.getPropertyDiscriminator(timestamp);
            List<Integer> propertyMetadata = elementData.propertyMetadata.computeIfAbsent(discriminator, k -> new ArrayList<>());
            IteratorMetadataEntry pme = new IteratorMetadataEntry(
                    propertyMetadataColumnQualifier.getMetadataKey(),
                    columnVisibility.toString(),
                    value.get()
            );
            int pos = elementData.metadataEntries.indexOf(pme);
            if (pos < 0) {
                pos = elementData.metadataEntries.size();
                elementData.metadataEntries.add(pme);
            }
            propertyMetadata.add(pos);
        }
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
        if (shouldIncludeProperty(propertyColumnQualifier.getPropertyName())) {
            this.elementData.propertyColumnQualifiers.put(mapKey, propertyColumnQualifier);
            this.elementData.propertyValues.put(mapKey, value.get());
            this.elementData.propertyVisibilities.put(mapKey, key.getColumnVisibility());
            this.elementData.propertyTimestamps.put(mapKey, timestamp);
        }
    }

    private boolean shouldIncludeProperty(String propertyName) {
        if (fetchHints.isIncludeAllProperties()) {
            return true;
        }
        return fetchHints.getPropertyNamesToInclude() != null
                && fetchHints.getPropertyNamesToInclude().contains(propertyName);
    }

    private boolean shouldIncludeMetadata(PropertyMetadataColumnQualifier propertyMetadataColumnQualifier) {
        if (!shouldIncludeProperty(propertyMetadataColumnQualifier.getPropertyName())) {
            return false;
        }
        if (fetchHints.isIncludeAllPropertyMetadata()) {
            return true;
        }
        String metadataKey = propertyMetadataColumnQualifier.getMetadataKey();
        return fetchHints.getMetadataKeysToInclude() != null
                && fetchHints.getMetadataKeysToInclude().contains(metadataKey);
    }

    @Override
    public abstract SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env);

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        fetchHints = new IteratorFetchHints(
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeAllProperties")),
                parseSet(options.get(SETTING_FETCH_HINTS_PREFIX + "propertyNamesToInclude")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeAllPropertyMetadata")),
                parseSet(options.get(SETTING_FETCH_HINTS_PREFIX + "metadataKeysToInclude")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeHidden")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeAllEdgeRefs")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeOutEdgeRefs")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeInEdgeRefs")),
                parseSet(options.get(SETTING_FETCH_HINTS_PREFIX + "edgeLabelsOfEdgeRefsToInclude")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeEdgeLabelsAndCounts")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeExtendedDataTableNames"))
        );
        elementData = createElementData();
    }

    protected abstract T createElementData();

    public static void setFetchHints(IteratorSetting iteratorSettings, IteratorFetchHints fetchHints) {
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeAllProperties", Boolean.toString(fetchHints.isIncludeAllProperties()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "propertyNamesToInclude", setToString(fetchHints.getPropertyNamesToInclude()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeAllPropertyMetadata", Boolean.toString(fetchHints.isIncludeAllPropertyMetadata()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "metadataKeysToInclude", setToString(fetchHints.getMetadataKeysToInclude()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeHidden", Boolean.toString(fetchHints.isIncludeHidden()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeAllEdgeRefs", Boolean.toString(fetchHints.isIncludeAllEdgeRefs()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeOutEdgeRefs", Boolean.toString(fetchHints.isIncludeOutEdgeRefs()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeInEdgeRefs", Boolean.toString(fetchHints.isIncludeInEdgeRefs()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "edgeLabelsOfEdgeRefsToInclude", setToString(fetchHints.getEdgeLabelsOfEdgeRefsToInclude()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeEdgeLabelsAndCounts", Boolean.toString(fetchHints.isIncludeEdgeLabelsAndCounts()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeExtendedDataTableNames", Boolean.toString(fetchHints.isIncludeExtendedDataTableNames()));
    }

    private static void addOption(IteratorSetting iteratorSettings, String key, String value) {
        if (value == null) {
            return;
        }
        iteratorSettings.addOption(key, value);
    }

    private Set<String> parseSet(String str) {
        if (str == null) {
            return null;
        }
        String[] parts = RECORD_SEPARATOR_PATTERN.split(str);
        Set<String> results = new HashSet<>();
        Collections.addAll(results, parts);
        return results;
    }

    public static String setToString(Set<String> set) {
        if (set == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : set) {
            if (!first) {
                sb.append(RECORD_SEPARATOR);
            }
            sb.append(s);
            first = false;
        }
        return sb.toString();
    }

    public IteratorFetchHints getFetchHints() {
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
        if (populateElementData(keys, values)) {
            return this.getElementData();
        } else {
            return null;
        }
    }
}
