package org.vertexium;

import java.util.List;
import java.util.stream.Collectors;

public interface MetadataPlugin {
    /**
     * Determine if this metadata value should be written to the data store. Possible reasons to exclude the metadata
     * could include common repetitive values.
     *
     * @param elementId          The element id of the element containing the property which contains the metadata to be written
     * @param property           The property containing the metadata to be written
     * @param metadataKey        The key of the metadata being written
     * @param metadataVisibility The visibility of the metadata being written
     * @param metadataValue      The value of the metadata being written
     * @param propertyTimestamp  The timestamp of the property, this could be different than the timestamp of the
     *                           property passed in as a parameter
     * @return true, to write the metadata to storage. false, to skip writing the metadata to storage.
     */
    boolean shouldWriteMetadata(
        ElementId elementId,
        Property property,
        String metadataKey,
        Visibility metadataVisibility,
        Object metadataValue,
        long propertyTimestamp
    );

    List<Metadata.Entry> getAllDefaultEntries(long propertyTimestamp, FetchHints fetchHints);

    default List<Metadata.Entry> getDefaultEntriesForKey(String metadataKey, long propertyTimestamp) {
        return getAllDefaultEntries(propertyTimestamp, FetchHints.ALL).stream()
            .filter(entry -> entry.getKey().equals(metadataKey))
            .collect(Collectors.toList());
    }

    default Metadata.Entry getDefaultEntryForKey(String metadataKey, long propertyTimestamp) {
        return getDefaultEntriesForKey(metadataKey, propertyTimestamp).stream()
            .findFirst()
            .orElse(null);
    }

    default Metadata.Entry getDefaultEntryForKeyAndVisibility(
        String metadataKey,
        Visibility metadataVisibility,
        long propertyTimestamp
    ) {
        return getDefaultEntriesForKey(metadataKey, propertyTimestamp).stream()
            .filter(entry -> entry.getVisibility().equals(metadataVisibility))
            .findFirst()
            .orElse(null);
    }
}
