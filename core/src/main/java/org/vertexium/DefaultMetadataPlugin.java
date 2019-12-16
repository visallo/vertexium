package org.vertexium;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultMetadataPlugin implements MetadataPlugin {
    @Override
    public boolean shouldWriteMetadata(
        ElementId elementId,
        Property property,
        String metadataKey,
        Visibility metadataVisibility,
        Object metadataValue,
        long propertyTimestamp
    ) {
        return true;
    }

    @Override
    public List<Metadata.Entry> getAllDefaultEntries(long propertyTimestamp, FetchHints fetchHints) {
        return Collections.emptyList();
    }

    protected List<Metadata.Entry> filterEntriesByFetchHints(List<Metadata.Entry> entries, FetchHints fetchHints) {
        return entries.stream()
            .filter(entry -> fetchHints.isIncludeMetadata(entry.getKey()))
            .collect(Collectors.toList());
    }
}
