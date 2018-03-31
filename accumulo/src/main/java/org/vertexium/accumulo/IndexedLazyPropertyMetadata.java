package org.vertexium.accumulo;

import org.vertexium.*;
import org.vertexium.id.NameSubstitutionStrategy;

import java.util.List;

public class IndexedLazyPropertyMetadata extends LazyPropertyMetadata {
    private final List<MetadataEntry> metadataEntries;
    private final int[] metadataIndexes;

    public IndexedLazyPropertyMetadata(List<MetadataEntry> metadataEntries, int[] metadataIndexes) {
        this.metadataEntries = metadataEntries;
        this.metadataIndexes = metadataIndexes;
    }

    public Metadata toMetadata(
            VertexiumSerializer vertexiumSerializer,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            FetchHints fetchHints
    ) {
        Metadata metadata = new Metadata(fetchHints);
        if (metadataIndexes == null) {
            return metadata;
        }
        for (int metadataIndex : metadataIndexes) {
            MetadataEntry entry = metadataEntries.get(metadataIndex);
            String metadataKey = entry.getMetadataKey(nameSubstitutionStrategy);
            Visibility metadataVisibility = entry.getVisibility();
            Object metadataValue = entry.getValue(vertexiumSerializer);
            if (metadataValue == null) {
                throw new VertexiumException("Invalid metadata value found.");
            }
            metadata.add(metadataKey, metadataValue, metadataVisibility);
        }
        return metadata;
    }
}
