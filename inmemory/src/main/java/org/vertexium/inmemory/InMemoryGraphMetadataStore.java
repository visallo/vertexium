package org.vertexium.inmemory;

import org.vertexium.GraphMetadataEntry;
import org.vertexium.GraphMetadataStore;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.JavaSerializableUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class InMemoryGraphMetadataStore extends GraphMetadataStore implements Serializable {
    private final Map<String, byte[]> metadata = new HashMap<>();

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        return new ConvertingIterable<Map.Entry<String, byte[]>, GraphMetadataEntry>(this.metadata.entrySet()) {
            @Override
            protected GraphMetadataEntry convert(Map.Entry<String, byte[]> o) {
                return new GraphMetadataEntry(o.getKey(), o.getValue());
            }
        };
    }

    @Override
    public Object getMetadata(String key) {
        byte[] bytes = this.metadata.get(key);
        if (bytes == null) {
            return null;
        }
        return JavaSerializableUtils.bytesToObject(bytes);
    }

    @Override
    public void setMetadata(String key, Object value) {
        this.metadata.put(key, JavaSerializableUtils.objectToBytes(value));
    }
}
