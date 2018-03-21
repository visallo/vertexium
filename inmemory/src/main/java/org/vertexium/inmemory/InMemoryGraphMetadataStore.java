package org.vertexium.inmemory;

import org.vertexium.GraphMetadataEntry;
import org.vertexium.GraphMetadataStore;
import org.vertexium.util.JavaSerializableUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class InMemoryGraphMetadataStore extends GraphMetadataStore implements Serializable {
    private final ReadWriteLock metadataLock = new ReentrantReadWriteLock();
    private final Map<String, byte[]> metadata = new HashMap<>();

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        metadataLock.readLock().lock();
        try {
            return this.metadata.entrySet().stream()
                    .map(o -> new GraphMetadataEntry(o.getKey(), o.getValue()))
                    .collect(Collectors.toList());
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public Object getMetadata(String key) {
        metadataLock.readLock().lock();
        try {
            byte[] bytes = this.metadata.get(key);
            if (bytes == null) {
                return null;
            }
            return JavaSerializableUtils.bytesToObject(bytes);
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public void setMetadata(String key, Object value) {
        metadataLock.writeLock().lock();
        try {
            this.metadata.put(key, JavaSerializableUtils.objectToBytes(value));
        } finally {
            metadataLock.writeLock().unlock();
        }
    }
}
