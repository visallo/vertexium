package org.vertexium;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MapMetadata implements Metadata {
    private static final String KEY_SEPARATOR = "\u001f";
    private final Map<String, Entry> entries;
    private final FetchHints fetchHints;
    private transient ReadWriteLock entriesLock = new ReentrantReadWriteLock();

    public MapMetadata() {
        this(FetchHints.ALL);
    }

    public MapMetadata(FetchHints fetchHints) {
        this.entries = new HashMap<>();
        this.fetchHints = fetchHints;
    }

    public MapMetadata(Metadata copyFromMetadata) {
        this(copyFromMetadata, FetchHints.ALL);
    }

    public MapMetadata(Metadata copyFromMetadata, FetchHints fetchHints) {
        this(fetchHints);
        if (copyFromMetadata != null) {
            for (Metadata.Entry entry : copyFromMetadata.entrySet()) {
                add(entry.getKey(), entry.getValue(), entry.getVisibility());
            }
        }
    }

    @Override
    public Metadata add(String key, Object value, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            entries.put(toMapKey(key, visibility), new Entry(key, value, visibility));
            return this;
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            entries.remove(toMapKey(key, visibility));
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        getEntriesLock().writeLock().lock();
        try {
            entries.clear();
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key) {
        getEntriesLock().writeLock().lock();
        try {
            for (Map.Entry<String, Entry> e : new ArrayList<>(entries.entrySet())) {
                if (e.getValue().getKey().equals(key)) {
                    entries.remove(e.getKey());
                }
            }
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public Collection<Metadata.Entry> entrySet() {
        getEntriesLock().readLock().lock();
        try {
            return new ArrayList<>(entries.values());
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Entry getEntry(String key, Visibility visibility) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            return entries.get(toMapKey(key, visibility));
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Entry getEntry(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            Entry entry = null;
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    if (entry != null) {
                        throw new VertexiumException("Multiple matching entries for key: " + key);
                    }
                    entry = e.getValue();
                }
            }
            return entry;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Collection<Metadata.Entry> getEntries(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            Collection<Metadata.Entry> results = new ArrayList<>();
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    Entry entry = e.getValue();
                    results.add(entry);
                }
            }
            return results;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    return true;
                }
            }
            return false;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    private String toMapKey(String key, Visibility visibility) {
        return key + KEY_SEPARATOR + visibility.getVisibilityString();
    }

    private ReadWriteLock getEntriesLock() {
        // entriesLock may be null if this class has just been deserialized
        if (entriesLock == null) {
            entriesLock = new ReentrantReadWriteLock();
        }
        return entriesLock;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    class Entry implements Metadata.Entry, Serializable {
        static final long serialVersionUID = 42L;
        private final String key;
        private final Object value;
        private final Visibility visibility;

        protected Entry(String key, Object value, Visibility visibility) {
            this.key = key;
            this.value = value;
            this.visibility = visibility;
        }

        public String getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Visibility getVisibility() {
            return visibility;
        }

        @Override
        public String toString() {
            return "Entry{" +
                "key='" + key + '\'' +
                ", value=" + value +
                ", visibility=" + visibility +
                '}';
        }
    }
}
