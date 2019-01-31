package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.*;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public abstract class ElementIterator<T extends ElementData> implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
    public static final String CF_PROPERTY_STRING = "PROP";
    public static final Text CF_PROPERTY = new Text(CF_PROPERTY_STRING);
    private static final byte[] CF_PROPERTY_BYTES = CF_PROPERTY.getBytes();

    public static final String CF_PROPERTY_HIDDEN_STRING = "PROPH";
    public static final Text CF_PROPERTY_HIDDEN = new Text(CF_PROPERTY_HIDDEN_STRING);
    private static final byte[] CF_PROPERTY_HIDDEN_BYTES = CF_PROPERTY_HIDDEN.getBytes();

    public static final String CF_PROPERTY_SOFT_DELETE_STRING = "PROPD";
    public static final Text CF_PROPERTY_SOFT_DELETE = new Text(CF_PROPERTY_SOFT_DELETE_STRING);
    private static final byte[] CF_PROPERTY_SOFT_DELETE_BYTES = CF_PROPERTY_SOFT_DELETE.getBytes();

    public static final String CF_PROPERTY_METADATA_STRING = "PROPMETA";
    public static final Text CF_PROPERTY_METADATA = new Text(CF_PROPERTY_METADATA_STRING);
    private static final byte[] CF_PROPERTY_METADATA_BYTES = CF_PROPERTY_METADATA.getBytes();

    public static final String CF_HIDDEN_STRING = "H";
    public static final Text CF_HIDDEN = new Text(CF_HIDDEN_STRING);
    private static final byte[] CF_HIDDEN_BYTES = CF_HIDDEN.getBytes();

    public static final Text CQ_HIDDEN = new Text("H");
    private static final byte[] CQ_HIDDEN_BYTES = CQ_HIDDEN.getBytes();

    public static final String CF_SOFT_DELETE_STRING = "D";
    public static final Text CF_SOFT_DELETE = new Text(CF_SOFT_DELETE_STRING);
    private static final byte[] CF_SOFT_DELETE_BYTES = CF_SOFT_DELETE.getBytes();

    public static final Text CQ_SOFT_DELETE = new Text("D");
    private static final byte[] CQ_SOFT_DELETE_BYTES = CQ_SOFT_DELETE.getBytes();

    public static final Value SOFT_DELETE_VALUE = new Value("".getBytes());

    public static final String CF_EXTENDED_DATA_STRING = "EXTDATA";
    public static final Text CF_EXTENDED_DATA = new Text(CF_EXTENDED_DATA_STRING);
    private static final byte[] CF_EXTENDED_DATA_BYTES = CF_EXTENDED_DATA.getBytes();

    public static final Value HIDDEN_VALUE = new Value("".getBytes());
    public static final Value HIDDEN_VALUE_DELETED = new Value("X".getBytes());

    public static final String DELETE_ROW_COLUMN_FAMILY_STRING = "";
    public static final Text DELETE_ROW_COLUMN_FAMILY = new Text(DELETE_ROW_COLUMN_FAMILY_STRING);
    private static final byte[] DELETE_ROW_COLUMN_FAMILY_BYTES = DELETE_ROW_COLUMN_FAMILY.getBytes();

    public static final String DELETE_ROW_COLUMN_QUALIFIER_STRING = "";
    public static final Text DELETE_ROW_COLUMN_QUALIFIER = new Text(DELETE_ROW_COLUMN_QUALIFIER_STRING);
    private static final byte[] DELETE_ROW_COLUMN_QUALIFIER_BYTES = DELETE_ROW_COLUMN_QUALIFIER.getBytes();

    public static final String METADATA_COLUMN_FAMILY_STRING = "";
    public static final Text METADATA_COLUMN_FAMILY = new Text(METADATA_COLUMN_FAMILY_STRING);
    private static final byte[] METADATA_COLUMN_FAMILY_BYTES = METADATA_COLUMN_FAMILY.getBytes();

    public static final String METADATA_COLUMN_QUALIFIER_STRING = "";
    public static final Text METADATA_COLUMN_QUALIFIER = new Text(METADATA_COLUMN_QUALIFIER_STRING);
    private static final byte[] METADATA_COLUMN_QUALIFIER_BYTES = METADATA_COLUMN_QUALIFIER.getBytes();

    private static final String SETTING_FETCH_HINTS_PREFIX = "fetchHints.";
    private static final String RECORD_SEPARATOR = "\u001f";
    private static final Pattern RECORD_SEPARATOR_PATTERN = Pattern.compile(Pattern.quote(RECORD_SEPARATOR));
    private SortedKeyValueIterator<Key, Value> sourceIterator;
    private IteratorFetchHints fetchHints;
    private T elementData;
    private Key topKey;
    private Value topValue;

    public ElementIterator(SortedKeyValueIterator<Key, Value> source, IteratorFetchHints fetchHints) {
        this.sourceIterator = source;
        this.fetchHints = fetchHints;
        this.elementData = createElementData();
    }

    @Override
    public boolean hasTop() {
        return topKey != null;
    }

    @Override
    public void next() throws IOException {
        topKey = null;
        topValue = null;
        loadNext();
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    /**
     * Copied from {@link org.apache.accumulo.core.iterators.user.RowEncodingIterator}
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        topKey = null;
        topValue = null;

        Key sk = range.getStartKey();

        if (sk != null
                && sk.getColumnFamilyData().length() == 0
                && sk.getColumnQualifierData().length() == 0
                && sk.getColumnVisibilityData().length() == 0
                && sk.getTimestamp() == Long.MAX_VALUE
                && !range.isStartKeyInclusive()) {
            // assuming that we are seeking using a key previously returned by this iterator therefore go the next row
            Key followingRowKey = sk.followingKey(PartialKey.ROW);
            if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0) {
                return;
            }

            range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
        }

        sourceIterator.seek(range, columnFamilies, inclusive);
        loadNext();
    }

    private void loadNext() throws IOException {
        if (topKey != null) {
            return;
        }
        while (sourceIterator.hasTop()) {
            Text currentRow = loadElement();
            if (currentRow != null) {
                topKey = new Key(currentRow);
                topValue = elementData.encode(fetchHints);
                break;
            }
        }
    }

    protected Text loadElement() throws IOException {
        this.elementData.clear();

        boolean deletedOrHidden = false;
        KeyValue keyValue = new KeyValue();
        Text currentRow = new Text(sourceIterator.getTopKey().getRow());
        Text row = new Text();
        while (sourceIterator.hasTop() && sourceIterator.getTopKey().getRow(row).equals(currentRow)) {
            if (!deletedOrHidden) {
                keyValue.set(sourceIterator.getTopKey(), sourceIterator.getTopValue());
                if (!processKeyValue(keyValue)) {
                    deletedOrHidden = true;
                }
            }
            sourceIterator.next();
        }

        if (deletedOrHidden) {
            return null;
        }

        if (this.elementData.visibility == null) {
            return null;
        }

        if (this.elementData.softDeleteTimestamp >= this.elementData.timestamp) {
            return null;
        }

        return currentRow;
    }

    private boolean processKeyValue(KeyValue keyValue) {
        if (this.elementData.id == null) {
            this.elementData.id = keyValue.takeRow();
        }

        if (keyValue.columnFamilyEquals(CF_PROPERTY_METADATA_BYTES)) {
            extractPropertyMetadata(keyValue);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_PROPERTY_BYTES)) {
            extractPropertyData(keyValue);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_EXTENDED_DATA_BYTES)) {
            this.elementData.extendedTableNames.add(keyValue.peekValue().toString());
            return true;
        }

        if (keyValue.columnFamilyEquals(getVisibilitySignal()) && keyValue.getTimestamp() > elementData.timestamp) {
            elementData.visibility = keyValue.takeColumnVisibility();
            elementData.timestamp = keyValue.getTimestamp();
            processSignalColumn(keyValue);
            return true;
        }

        if (processColumn(keyValue)) {
            return true;
        }

        if (keyValue.columnFamilyEquals(DELETE_ROW_COLUMN_FAMILY_BYTES)
                && keyValue.columnQualifierEquals(DELETE_ROW_COLUMN_QUALIFIER_BYTES)
                && RowDeletingIterator.DELETE_ROW_VALUE.equals(keyValue.peekValue())) {
            return false;
        }

        if (keyValue.columnFamilyEquals(CF_SOFT_DELETE_BYTES)
                && keyValue.columnQualifierEquals(CQ_SOFT_DELETE_BYTES)
                && SOFT_DELETE_VALUE.equals(keyValue.peekValue())) {
            elementData.softDeleteTimestamp = keyValue.getTimestamp();
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_PROPERTY_SOFT_DELETE_BYTES)) {
            extractPropertySoftDelete(keyValue);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_HIDDEN_BYTES)) {
            if (fetchHints.isIncludeHidden()) {
                this.elementData.hiddenVisibilities.add(keyValue.takeColumnVisibility());
                return true;
            } else {
                return false;
            }
        }

        if (keyValue.columnFamilyEquals(CF_PROPERTY_HIDDEN_BYTES)) {
            extractPropertyHidden(keyValue);
            return true;
        }

        return true;
    }

    protected abstract boolean processColumn(KeyValue keyValue);

    protected void processSignalColumn(KeyValue keyValue) {
    }

    public T getElementData() {
        return elementData;
    }

    protected abstract byte[] getVisibilitySignal();

    private void extractPropertySoftDelete(KeyValue keyValue) {
        PropertyColumnQualifierByteSequence propertyColumnQualifier =
                new PropertyColumnQualifierByteSequence(keyValue.takeColumnQualifierByteSequence());
        SoftDeletedProperty softDeletedProperty = new SoftDeletedProperty(
                propertyColumnQualifier.getPropertyKey(),
                propertyColumnQualifier.getPropertyName(),
                keyValue.getTimestamp(),
                keyValue.takeColumnVisibilityByteSequence()
        );
        this.elementData.softDeletedProperties.add(softDeletedProperty);
    }

    private void extractPropertyMetadata(KeyValue keyValue) {
        PropertyMetadataColumnQualifierByteSequence propertyMetadataColumnQualifier =
                new PropertyMetadataColumnQualifierByteSequence(keyValue.takeColumnQualifierByteSequence());
        if (shouldIncludeMetadata(propertyMetadataColumnQualifier)) {
            ByteSequence discriminator = propertyMetadataColumnQualifier.getPropertyDiscriminator(keyValue.getTimestamp());
            List<Integer> propertyMetadata = elementData.propertyMetadata.computeIfAbsent(discriminator, k -> new ArrayList<>());
            IteratorMetadataEntry pme = new IteratorMetadataEntry(
                    propertyMetadataColumnQualifier.getMetadataKey(),
                    keyValue.takeColumnVisibilityByteSequence(),
                    keyValue.takeValue().get()
            );
            int pos = elementData.metadataEntries.indexOf(pme);
            if (pos < 0) {
                pos = elementData.metadataEntries.size();
                elementData.metadataEntries.add(pme);
            }
            propertyMetadata.add(pos);
        }
    }

    private void extractPropertyHidden(KeyValue keyValue) {
        if (keyValue.peekValue().equals(HIDDEN_VALUE_DELETED)) {
            return;
        }
        PropertyHiddenColumnQualifierByteSequence propertyHiddenColumnQualifier =
                new PropertyHiddenColumnQualifierByteSequence(keyValue.takeColumnQualifierByteSequence());
        HiddenProperty hiddenProperty = new HiddenProperty(
                propertyHiddenColumnQualifier.getPropertyKey(),
                propertyHiddenColumnQualifier.getPropertyName(),
                propertyHiddenColumnQualifier.getPropertyVisibilityString(),
                keyValue.takeColumnVisibilityByteSequence()
        );
        this.elementData.hiddenProperties.add(hiddenProperty);
    }

    private void extractPropertyData(KeyValue keyValue) {
        PropertyColumnQualifierByteSequence propertyColumnQualifier =
                new PropertyColumnQualifierByteSequence(keyValue.takeColumnQualifierByteSequence());
        ByteSequence mapKey = propertyColumnQualifier.getDiscriminator(keyValue.peekColumnVisibilityByteSequence(), keyValue.getTimestamp());
        long timestamp = keyValue.getTimestamp();
        if (shouldIncludeProperty(propertyColumnQualifier.getPropertyName())) {
            this.elementData.propertyColumnQualifiers.put(mapKey, propertyColumnQualifier);
            this.elementData.propertyValues.put(mapKey, keyValue.takeValue().get());
            this.elementData.propertyVisibilities.put(mapKey, keyValue.takeColumnVisibilityByteSequence());
            this.elementData.propertyTimestamps.put(mapKey, timestamp);
        }
    }

    private boolean shouldIncludeProperty(ByteSequence propertyName) {
        if (fetchHints.isIncludeAllProperties()) {
            return true;
        }
        return fetchHints.getPropertyNamesToInclude() != null
                && fetchHints.getPropertyNamesToInclude().contains(propertyName);
    }

    private boolean shouldIncludeMetadata(PropertyMetadataColumnQualifierByteSequence propertyMetadataColumnQualifier) {
        if (!shouldIncludeProperty(propertyMetadataColumnQualifier.getPropertyName())) {
            return false;
        }
        if (fetchHints.isIncludeAllPropertyMetadata()) {
            return true;
        }
        ByteSequence metadataKey = propertyMetadataColumnQualifier.getMetadataKey();
        return fetchHints.getMetadataKeysToInclude() != null
                && fetchHints.getMetadataKeysToInclude().contains(metadataKey);
    }

    @Override
    public abstract SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env);

    @Override
    public IteratorOptions describeOptions() {
        Map<String, String> namedOptions = new HashMap<>();
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeAllProperties", "true to include all properties");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "propertyNamesToInclude", "Set of property names to include separated by \\u001f");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeAllPropertyMetadata", "true to include all property metadata");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "metadataKeysToInclude", "Set of metadata keys to include separated by \\u001f");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeHidden", "true to include hidden data");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeAllEdgeRefs", "true to include all edge refs");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeOutEdgeRefs", "true to include out edge refs");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeInEdgeRefs", "true to include in edge refs");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "edgeLabelsOfEdgeRefsToInclude", "Set of edge labels to include separated by \\u001f");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeEdgeLabelsAndCounts", "true to include edge labels with counts");
        namedOptions.put(SETTING_FETCH_HINTS_PREFIX + "includeExtendedDataTableNames", "true to include extended data table names");
        return new IteratorOptions(getClass().getSimpleName(), getDescription(), namedOptions, null);
    }

    protected abstract String getDescription();

    @Override
    public boolean validateOptions(Map<String, String> options) {
        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) {
        this.sourceIterator = source;
        fetchHints = new IteratorFetchHints(
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeAllProperties")),
                parseTextSet(options.get(SETTING_FETCH_HINTS_PREFIX + "propertyNamesToInclude")),
                Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includeAllPropertyMetadata")),
                parseTextSet(options.get(SETTING_FETCH_HINTS_PREFIX + "metadataKeysToInclude")),
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

    public SortedKeyValueIterator<Key, Value> getSourceIterator() {
        return sourceIterator;
    }

    private Set<ByteSequence> parseTextSet(String str) {
        if (str == null) {
            return null;
        }
        String[] parts = RECORD_SEPARATOR_PATTERN.split(str);
        Set<ByteSequence> results = new HashSet<>();
        for (String part : parts) {
            results.add(new ArrayByteSequence(part));
        }
        return results;
    }

    protected abstract T createElementData();

    public static void setFetchHints(IteratorSetting iteratorSettings, IteratorFetchHints fetchHints) {
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeAllProperties", Boolean.toString(fetchHints.isIncludeAllProperties()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "propertyNamesToInclude", textSetToString(fetchHints.getPropertyNamesToInclude()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeAllPropertyMetadata", Boolean.toString(fetchHints.isIncludeAllPropertyMetadata()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "metadataKeysToInclude", textSetToString(fetchHints.getMetadataKeysToInclude()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeHidden", Boolean.toString(fetchHints.isIncludeHidden()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeAllEdgeRefs", Boolean.toString(fetchHints.isIncludeAllEdgeRefs()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeOutEdgeRefs", Boolean.toString(fetchHints.isIncludeOutEdgeRefs()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeInEdgeRefs", Boolean.toString(fetchHints.isIncludeInEdgeRefs()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "edgeLabelsOfEdgeRefsToInclude", setToString(fetchHints.getEdgeLabelsOfEdgeRefsToInclude()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeEdgeLabelsAndCounts", Boolean.toString(fetchHints.isIncludeEdgeLabelsAndCounts()));
        addOption(iteratorSettings, SETTING_FETCH_HINTS_PREFIX + "includeExtendedDataTableNames", Boolean.toString(fetchHints.isIncludeExtendedDataTableNames()));
    }

    private static String textSetToString(Set<ByteSequence> set) {
        if (set == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (ByteSequence s : set) {
            if (!first) {
                sb.append(RECORD_SEPARATOR);
            }
            sb.append(ByteSequenceUtils.toString(s));
            first = false;
        }
        return sb.toString();
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

    protected boolean populateElementData(List<Key> keys, List<Value> values) {
        this.elementData.clear();

        KeyValue keyValue = new KeyValue();
        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            Value value = values.get(i);
            keyValue.set(key, value);
            if (!processKeyValue(keyValue)) {
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
