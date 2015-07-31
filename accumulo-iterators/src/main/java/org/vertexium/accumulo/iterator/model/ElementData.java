package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public abstract class ElementData {
    public static final Charset CHARSET = Charset.forName("utf8");
    public static final byte[] HEADER = new byte[]{'V', 'E', 'R', 'T', '1'};
    public static final byte TYPE_ID_VERTEX = 1;
    public static final byte TYPE_ID_EDGE = 2;
    public static final int PROP_START = 1;
    public static final int PROP_END = 2;
    public Text id;
    public long timestamp;
    public Text visibility;
    public List<Text> hiddenVisibilities = new ArrayList<>();
    public long softDeleteTimestamp;
    public List<SoftDeletedProperty> softDeletedProperties = new ArrayList<>();
    public List<HiddenProperty> hiddenProperties = new ArrayList<>();
    public Map<String, PropertyMetadata> propertyMetadata = new HashMap<>();
    public final Map<String, PropertyColumnQualifier> propertyColumnQualifiers = new HashMap<>();
    public final Map<String, byte[]> propertyValues = new HashMap<>();
    public final Map<String, Text> propertyVisibilities = new HashMap<>();
    public final Map<String, Long> propertyTimestamps = new HashMap<>();

    public void clear() {
        id = null;
        visibility = null;
        timestamp = 0;
        softDeleteTimestamp = 0;
        hiddenVisibilities.clear();
        softDeletedProperties.clear();
        hiddenProperties.clear();
        propertyMetadata.clear();
        propertyColumnQualifiers.clear();
        propertyValues.clear();
        propertyVisibilities.clear();
        propertyTimestamps.clear();
    }

    public final Value encode(EnumSet<FetchHint> fetchHints) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        encode(dout, fetchHints);
        return new Value(out.toByteArray());
    }

    protected void encode(DataOutputStream out, EnumSet<FetchHint> fetchHints) throws IOException {
        encodeHeader(out);
        encodeText(out, id);
        out.writeLong(timestamp);
        encodeText(out, visibility);
        encodeTextList(out, hiddenVisibilities);
        encodeProperties(out, fetchHints);
    }

    private void encodeHeader(DataOutputStream out) throws IOException {
        out.write(HEADER);
        out.write(getTypeId());
    }

    protected abstract byte getTypeId();

    protected void encodeTextList(DataOutputStream out, Collection<Text> texts) throws IOException {
        if (texts == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(texts.size());
        for (Text text : texts) {
            encodeText(out, text);
        }
    }

    protected void encodeText(DataOutputStream out, Text text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(text.getLength());
        out.write(text.getBytes(), 0, text.getLength());
    }

    protected void encodeByteArray(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    protected void encodeString(DataOutputStream out, String text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = text.getBytes(CHARSET);
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    private void encodeProperties(final DataOutputStream out, EnumSet<FetchHint> fetchHints) throws IOException {
        iterateProperties(new PropertyDataHandler() {
            @Override
            public void handle(
                    String propertyKey,
                    String propertyName,
                    byte[] propertyValue,
                    Text propertyVisibility,
                    long propertyTimestamp,
                    Set<Text> propertyHiddenVisibilities,
                    PropertyMetadata metadata
            ) throws IOException {
                out.write(PROP_START);
                encodeString(out, propertyKey);
                encodeString(out, propertyName);
                encodeText(out, propertyVisibility);
                out.writeLong(propertyTimestamp);
                out.writeInt(propertyValue.length);
                out.write(propertyValue);
                encodeTextList(out, propertyHiddenVisibilities);
                encodePropertyMetadata(out, metadata);
            }
        }, fetchHints);
        out.write(PROP_END);
    }

    private void iterateProperties(PropertyDataHandler propertyDataHandler, EnumSet<FetchHint> fetchHints) throws IOException {
        boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);
        for (Map.Entry<String, byte[]> propertyValueEntry : propertyValues.entrySet()) {
            String key = propertyValueEntry.getKey();
            PropertyColumnQualifier propertyColumnQualifier = propertyColumnQualifiers.get(key);
            String propertyKey = propertyColumnQualifier.getPropertyKey();
            String propertyName = propertyColumnQualifier.getPropertyName();
            byte[] propertyValue = propertyValueEntry.getValue();
            Text propertyVisibility = propertyVisibilities.get(key);
            String propertyVisibilityString = propertyVisibility.toString();
            long propertyTimestamp = propertyTimestamps.get(key);
            Set<Text> propertyHiddenVisibilities = getPropertyHiddenVisibilities(propertyKey, propertyName, propertyVisibilityString);
            if (!includeHidden && isHidden(propertyKey, propertyName, propertyVisibilityString)) {
                continue;
            }
            if (isPropertyDeleted(propertyKey, propertyName, propertyTimestamp, propertyVisibility)) {
                continue;
            }
            PropertyMetadata metadata = propertyMetadata.get(key);
            propertyDataHandler.handle(propertyKey, propertyName, propertyValue, propertyVisibility, propertyTimestamp, propertyHiddenVisibilities, metadata);
        }
    }

    public Iterable<Property> getProperties(EnumSet<FetchHint> fetchHints) {
        final List<Property> results = new ArrayList<>();
        try {
            iterateProperties(new PropertyDataHandler() {
                @Override
                public void handle(
                        String propertyKey,
                        String propertyName,
                        byte[] propertyValue,
                        Text propertyVisibility,
                        long propertyTimestamp,
                        Set<Text> propertyHiddenVisibilities,
                        PropertyMetadata metadata
                ) throws IOException {
                    results.add(new Property(
                            propertyKey,
                            propertyName,
                            propertyValue,
                            propertyVisibility.toString(),
                            propertyTimestamp,
                            propertyHiddenVisibilities,
                            metadata
                    ));
                }
            }, fetchHints);
        } catch (IOException ex) {
            throw new VertexiumAccumuloIteratorException("Could not get properties", ex);
        }
        return results;
    }

    private interface PropertyDataHandler {
        void handle(String propertyKey, String propertyName, byte[] propertyValue, Text propertyVisibility, long propertyTimestamp, Set<Text> propertyHiddenVisibilities, PropertyMetadata metadata) throws IOException;
    }

    private void encodePropertyMetadata(DataOutputStream out, PropertyMetadata metadata) throws IOException {
        if (metadata == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(metadata.entries.size());
        for (Map.Entry<String, PropertyMetadata.Entry> propertyMetadata : metadata.entries.entrySet()) {
            encodePropertyMetadataEntry(out, propertyMetadata.getValue());
        }
    }

    private void encodePropertyMetadataEntry(DataOutputStream out, PropertyMetadata.Entry metadataEntry) throws IOException {
        encodeString(out, metadataEntry.metadataKey);
        encodeString(out, metadataEntry.metadataVisibility);
        out.writeInt(metadataEntry.value.length);
        out.write(metadataEntry.value);
    }

    private Set<Text> getPropertyHiddenVisibilities(String propertyKey, String propertyName, String propertyVisibility) {
        Set<Text> hiddenVisibilities = null;
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

    private boolean isHidden(String propertyKey, String propertyName, String propertyVisibility) {
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return true;
            }
        }
        return false;
    }

    private boolean isPropertyDeleted(String propertyKey, String propertyName, long propertyTimestamp, Text propertyVisibility) {
        for (SoftDeletedProperty softDeletedProperty : softDeletedProperties) {
            if (softDeletedProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return softDeletedProperty.getTimestamp() >= propertyTimestamp;
            }
        }
        return false;
    }
}
