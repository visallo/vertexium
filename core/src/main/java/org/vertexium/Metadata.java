package org.vertexium;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Metadata {
    public static final String KEY_SEPARATOR = "\u001f";

    private final Map<String, Entry> entries;
    private final FetchHints fetchHints;
    private transient ReadWriteLock entriesLock = new ReentrantReadWriteLock();

    public Metadata() {
        this(FetchHints.ALL);
    }

    public Metadata(FetchHints fetchHints) {
        this.entries = new HashMap<>();
        this.fetchHints = fetchHints;
    }

    public Metadata(Metadata copyFromMetadata) {
        this(copyFromMetadata, FetchHints.ALL);
    }

    public Metadata(Metadata copyFromMetadata, FetchHints fetchHints) {
        this(fetchHints);
        if (copyFromMetadata != null) {
            entries.putAll(copyFromMetadata.entries);
        }
    }

    public Metadata add(String key, Object value, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            entries.put(toMapKey(key, visibility), new Entry(key, value, visibility));
            return this;
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    public void remove(String key, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            entries.remove(toMapKey(key, visibility));
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    public void clear() {
        getEntriesLock().writeLock().lock();
        try {
            entries.clear();
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

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

    public Collection<Entry> entrySet() {
        getEntriesLock().readLock().lock();
        try {
            return new ArrayList<>(entries.values());
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    public Entry getEntry(String key, Visibility visibility) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            return entries.get(toMapKey(key, visibility));
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

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

    public Collection<Entry> getEntries(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            Collection<Entry> results = new ArrayList<>();
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

    public Object getValue(String key, Visibility visibility) {
        Entry entry = getEntry(key, visibility);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    public Object getValue(String key) {
        Entry entry = getEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    public Collection<Object> getValues(String key) {
        Collection<Object> results = new ArrayList<>();
        Collection<Entry> entries = getEntries(key);
        for (Entry entry : entries) {
            results.add(entry.getValue());
        }
        return results;
    }

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

    public boolean contains(String key, Visibility visibility) {
        return getEntry(key, visibility) != null;
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

    public FetchHints getFetchHints() {
        return fetchHints;
    }

    public static class Entry implements Serializable {
        static final long serialVersionUID = 42L;
        private final String key;
        private final Object value;
        private final Visibility visibility;

        private Entry(String key, Object value, Visibility visibility) {
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
