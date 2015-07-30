package org.vertexium.accumulo.iterator.model;

import java.util.HashMap;
import java.util.Map;

public class PropertyMetadata {
    public Map<String, Entry> entries = new HashMap<>();

    public void add(String metadataKey, String columnVisibility, byte[] value) {
        this.entries.put(metadataKey.concat(columnVisibility), new Entry(metadataKey, columnVisibility, value));
    }

    public static class Entry {
        public final String metadataKey;
        public final String metadataVisibility;
        public final byte[] value;

        public Entry(String metadataKey, String metadataVisibility, byte[] value) {
            this.metadataKey = metadataKey;
            this.metadataVisibility = metadataVisibility;
            this.value = value;
        }
    }
}
