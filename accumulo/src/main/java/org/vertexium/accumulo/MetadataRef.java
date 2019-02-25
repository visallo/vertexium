package org.vertexium.accumulo;

import java.util.List;

public class MetadataRef {
    private final List<MetadataEntry> metadataEntries;
    private final int[] metadataIndexes;

    public MetadataRef(List<MetadataEntry> metadataEntries, int[] metadataIndexes) {
        this.metadataEntries = metadataEntries;
        this.metadataIndexes = metadataIndexes;
    }

    public List<MetadataEntry> getMetadataEntries() {
        return metadataEntries;
    }

    public int[] getMetadataIndexes() {
        return metadataIndexes;
    }
}
