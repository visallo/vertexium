package org.vertexium.accumulo.iterator.model;

import com.google.protobuf.ByteArrayByteString;
import com.google.protobuf.ByteSequenceByteString;
import com.google.protobuf.TextByteString;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.proto.Element;
import org.vertexium.accumulo.iterator.model.proto.MetadataEntry;

import java.util.*;

public abstract class ElementData {
    public Text id;
    public long timestamp;
    public Text visibility;
    public final List<Text> hiddenVisibilities = new ArrayList<>();
    public long softDeleteTimestamp;
    public final List<SoftDeletedProperty> softDeletedProperties = new ArrayList<>();
    public final List<HiddenProperty> hiddenProperties = new ArrayList<>();
    public final List<IteratorMetadataEntry> metadataEntries = new ArrayList<>();
    public final Map<ByteSequence, List<Integer>> propertyMetadata = new HashMap<>();
    public final Map<ByteSequence, PropertyColumnQualifierByteSequence> propertyColumnQualifiers = new HashMap<>();
    public final Map<ByteSequence, byte[]> propertyValues = new HashMap<>();
    public final Map<ByteSequence, ByteSequence> propertyVisibilities = new HashMap<>();
    public final Map<ByteSequence, Long> propertyTimestamps = new HashMap<>();
    public final Set<Text> additionalVisibilities = new HashSet<>();
    public final Set<String> extendedTableNames = new HashSet<>();
    public boolean deleted;
    public boolean hidden;

    public void clear() {
        id = null;
        visibility = null;
        timestamp = 0;
        softDeleteTimestamp = 0;
        deleted = false;
        hidden = false;
        hiddenVisibilities.clear();
        softDeletedProperties.clear();
        hiddenProperties.clear();
        metadataEntries.clear();
        propertyMetadata.clear();
        propertyColumnQualifiers.clear();
        propertyValues.clear();
        propertyVisibilities.clear();
        propertyTimestamps.clear();
        additionalVisibilities.clear();
        extendedTableNames.clear();
    }

    public abstract Value encode(IteratorFetchHints fetchHints);

    protected Element encodeElement(IteratorFetchHints fetchHints) {
        Element.Builder builder = Element.newBuilder();
        builder.setId(new TextByteString(id));
        builder.setTimestamp(timestamp);
        builder.setVisibility(new TextByteString(visibility));
        for (Text hiddenVisibility : hiddenVisibilities) {
            builder.addHiddenVisibilities(new TextByteString(hiddenVisibility));
        }
        for (Text additionalVisibility : additionalVisibilities) {
            builder.addAdditionalVisibilities(new TextByteString(additionalVisibility));
        }
        encodePropertyMetadataLookup(builder);
        encodeProperties(builder, fetchHints);
        builder.addAllExtendedTableNames(extendedTableNames);
        return builder.build();
    }

    private void encodePropertyMetadataLookup(Element.Builder builder) {
        for (IteratorMetadataEntry metadataEntry : metadataEntries) {
            builder.addMetadataEntries(MetadataEntry.newBuilder()
                .setKey(new ByteSequenceByteString(metadataEntry.metadataKey))
                .setVisibility(new ByteSequenceByteString(metadataEntry.metadataVisibility))
                .setValue(new ByteArrayByteString(metadataEntry.value))
                .build());
        }
    }

    private void encodeProperties(Element.Builder builder, IteratorFetchHints fetchHints) {
        iterateProperties((
            propertyKey,
            propertyName,
            propertyValue,
            propertyVisibility,
            propertyTimestamp,
            propertyHiddenVisibilities,
            metadata
        ) -> {
            org.vertexium.accumulo.iterator.model.proto.Property.Builder propBuilder = org.vertexium.accumulo.iterator.model.proto.Property.newBuilder()
                .setKey(new ByteSequenceByteString(propertyKey))
                .setName(new ByteSequenceByteString(propertyName))
                .setVisibility(new ByteSequenceByteString(propertyVisibility))
                .setTimestamp(propertyTimestamp)
                .setValue(new ByteArrayByteString(propertyValue));
            if (metadata != null) {
                propBuilder.addAllMetadata(metadata);
            }
            if (propertyHiddenVisibilities != null) {
                for (ByteSequence propertyHiddenVisibility : propertyHiddenVisibilities) {
                    propBuilder.addHiddenVisibilities(new ByteSequenceByteString(propertyHiddenVisibility));
                }
            }
            builder.addProperties(propBuilder.build());
        }, fetchHints);
    }

    private void iterateProperties(PropertyDataHandler propertyDataHandler, IteratorFetchHints fetchHints) {
        boolean includeHidden = fetchHints.isIncludeHidden();
        for (Map.Entry<ByteSequence, byte[]> propertyValueEntry : propertyValues.entrySet()) {
            ByteSequence key = propertyValueEntry.getKey();
            PropertyColumnQualifierByteSequence propertyColumnQualifier = propertyColumnQualifiers.get(key);
            ByteSequence propertyKey = propertyColumnQualifier.getPropertyKey();
            ByteSequence propertyName = propertyColumnQualifier.getPropertyName();
            byte[] propertyValue = propertyValueEntry.getValue();
            ByteSequence propertyVisibility = propertyVisibilities.get(key);
            long propertyTimestamp = propertyTimestamps.get(key);
            if (propertyTimestamp < softDeleteTimestamp) {
                continue;
            }
            Set<ByteSequence> propertyHiddenVisibilities = getPropertyHiddenVisibilities(propertyKey, propertyName, propertyVisibility);
            if (!includeHidden && isHidden(propertyKey, propertyName, propertyVisibility)) {
                continue;
            }
            if (isPropertyDeleted(propertyKey, propertyName, propertyTimestamp, propertyVisibility)) {
                continue;
            }
            ByteSequence metadataKey = key;
            if (fetchHints.isIncludePreviousMetadata()) {
                metadataKey = KeyBaseByteSequence.discriminatorWithoutTimestamp(metadataKey);
            }
            List<Integer> metadata = propertyMetadata.get(metadataKey);
            propertyDataHandler.handle(
                propertyKey,
                propertyName,
                propertyValue,
                propertyVisibility,
                propertyTimestamp,
                propertyHiddenVisibilities,
                metadata
            );
        }
    }

    public Iterable<Property> getProperties(IteratorFetchHints fetchHints) {
        List<Property> results = new ArrayList<>();
        iterateProperties((
            propertyKey,
            propertyName,
            propertyValue,
            propertyVisibility,
            propertyTimestamp,
            propertyHiddenVisibilities,
            metadata
        ) -> results.add(new Property(
            propertyKey,
            propertyName,
            propertyValue,
            propertyVisibility,
            propertyTimestamp,
            propertyHiddenVisibilities,
            metadata
        )), fetchHints);
        return results;
    }

    public boolean isDeletedOrHidden() {
        return deleted || hidden;
    }

    private interface PropertyDataHandler {
        void handle(
            ByteSequence propertyKey,
            ByteSequence propertyName,
            byte[] propertyValue,
            ByteSequence propertyVisibility,
            long propertyTimestamp,
            Set<ByteSequence> propertyHiddenVisibilities,
            List<Integer> metadata
        );
    }

    private Set<ByteSequence> getPropertyHiddenVisibilities(
        ByteSequence propertyKey,
        ByteSequence propertyName,
        ByteSequence propertyVisibility
    ) {
        Set<ByteSequence> hiddenVisibilities = null;
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

    private boolean isHidden(ByteSequence propertyKey, ByteSequence propertyName, ByteSequence propertyVisibility) {
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return true;
            }
        }
        return false;
    }

    private boolean isPropertyDeleted(ByteSequence propertyKey, ByteSequence propertyName, long propertyTimestamp, ByteSequence propertyVisibility) {
        for (SoftDeletedProperty softDeletedProperty : softDeletedProperties) {
            if (softDeletedProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return softDeletedProperty.getTimestamp() >= propertyTimestamp;
            }
        }
        return false;
    }
}
