package org.vertexium;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Metadata implements Serializable {
    static final long serialVersionUID = 42L;
    public static final String KEY_SEPARATOR = "\u001f";

    private final Map<String, Entry> entries;
    private final transient ReadWriteLock entiresLock = new ReentrantReadWriteLock();

    public Metadata() {
        entries = new HashMap<>();
    }

    public Metadata(Metadata copyFromMetadata) {
        this();
        if (copyFromMetadata != null) {
            entries.putAll(copyFromMetadata.entries);
        }
    }

    public Metadata add(String key, Object value, Visibility visibility) {
        entiresLock.writeLock().lock();
        try {
            entries.put(toMapKey(key, visibility), new Entry(key, value, visibility));
            return this;
        } finally {
            entiresLock.writeLock().unlock();
        }
    }

    public void remove(String key, Visibility visibility) {
        entiresLock.writeLock().lock();
        try {
            entries.remove(toMapKey(key, visibility));
        } finally {
            entiresLock.writeLock().unlock();
        }
    }

    public void clear() {
        entiresLock.writeLock().lock();
        try {
            entries.clear();
        } finally {
            entiresLock.writeLock().unlock();
        }
    }

    public void remove(String key) {
        entiresLock.writeLock().lock();
        try {
            for (Map.Entry<String, Entry> e : new ArrayList<>(entries.entrySet())) {
                if (e.getValue().getKey().equals(key)) {
                    entries.remove(e.getKey());
                }
            }
        } finally {
            entiresLock.writeLock().unlock();
        }
    }

    public Collection<Entry> entrySet() {
        entiresLock.readLock().lock();
        try {
            return new ArrayList<>(entries.values());
        } finally {
            entiresLock.readLock().unlock();
        }
    }

    public Entry getEntry(String key, Visibility visibility) {
        entiresLock.readLock().lock();
        try {
            return entries.get(toMapKey(key, visibility));
        } finally {
            entiresLock.readLock().unlock();
        }
    }

    public Entry getEntry(String key) {
        entiresLock.readLock().lock();
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
            entiresLock.readLock().unlock();
        }
    }

    public Collection<Entry> getEntries(String key) {
        entiresLock.readLock().lock();
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
            entiresLock.readLock().unlock();
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
        entiresLock.readLock().lock();
        try {
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    return true;
                }
            }
            return false;
        } finally {
            entiresLock.readLock().unlock();
        }
    }

    public boolean contains(String key, Visibility visibility) {
        return getEntry(key, visibility) != null;
    }

    private String toMapKey(String key, Visibility visibility) {
        return key + KEY_SEPARATOR + visibility.getVisibilityString();
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
