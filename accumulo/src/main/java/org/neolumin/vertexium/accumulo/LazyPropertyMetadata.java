package org.neolumin.vertexium.accumulo;

import org.neolumin.vertexium.Metadata;
import org.neolumin.vertexium.VertexiumException;
import org.neolumin.vertexium.Visibility;
import org.neolumin.vertexium.accumulo.serializer.ValueSerializer;

import java.util.HashMap;
import java.util.Map;

public class LazyPropertyMetadata {
    private Map<String, Entry> entries = new HashMap<String, Entry>();

    public Metadata toMetadata(ValueSerializer valueSerializer) {
        Metadata metadata = new Metadata();
        for (Map.Entry<String, Entry> metadataItem : this.entries.entrySet()) {
            String metadataKey = metadataItem.getValue().getMetadataKey();
            Visibility metadataVisibility = metadataItem.getValue().getMetadataVisibility();
            Object metadataValue = valueSerializer.valueToObject(metadataItem.getValue().getValue());
            if (metadataValue == null) {
                throw new VertexiumException("Invalid metadata found.");
            }
            metadata.add(metadataKey, metadataValue, metadataVisibility);
        }
        return metadata;
    }

    public void add(String metadataKey, Visibility metadataVisibility, byte[] value) {
        this.entries.put(metadataKey + metadataVisibility.getVisibilityString(), new Entry(metadataKey, metadataVisibility, value));
    }

    private static class Entry {
        private final String metadataKey;
        private final Visibility metadataVisibility;
        private final byte[] value;

        public Entry(String metadataKey, Visibility metadataVisibility, byte[] value) {
            this.metadataKey = metadataKey;
            this.metadataVisibility = metadataVisibility;
            this.value = value;
        }

        public String getMetadataKey() {
            return metadataKey;
        }

        public Visibility getMetadataVisibility() {
            return metadataVisibility;
        }

        public byte[] getValue() {
            return value;
        }
    }
}
