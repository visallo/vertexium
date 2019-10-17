package org.vertexium.accumulo;

import org.vertexium.*;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LazyPropertyMetadata implements Metadata {
    private static final String KEY_SEPARATOR = "\u001f";
    private transient ReadWriteLock entriesLock = new ReentrantReadWriteLock();
    private final Map<String, Metadata.Entry> entries = new HashMap<>();
    private final List<MetadataEntry> metadataEntries;
    private int[] metadataIndexes;
    private final Set<String> removedEntries = new HashSet<>();
    private final VertexiumSerializer vertexiumSerializer;
    private final AccumuloNameSubstitutionStrategy nameSubstitutionStrategy;
    private final FetchHints fetchHints;

    public LazyPropertyMetadata(
        List<MetadataEntry> metadataEntries,
        int[] metadataIndexes,
        VertexiumSerializer vertexiumSerializer,
        AccumuloNameSubstitutionStrategy nameSubstitutionStrategy,
        FetchHints fetchHints
    ) {
        this.metadataEntries = metadataEntries;
        this.metadataIndexes = metadataIndexes;
        this.vertexiumSerializer = vertexiumSerializer;
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
        this.fetchHints = fetchHints;
    }

    @Override
    public Metadata add(String key, Object value, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            String mapKey = toMapKey(key, visibility);
            removedEntries.remove(mapKey);
            entries.put(mapKey, new Entry(key, value, visibility));
            return this;
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            String mapKey = toMapKey(key, visibility);
            removedEntries.add(mapKey);
            entries.remove(mapKey);
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key) {
        getEntriesLock().writeLock().lock();
        try {
            if (metadataIndexes != null && metadataEntries != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry entry = metadataEntries.get(metadataIndex);
                    String metadataKey = entry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = entry.getVisibility();
                        String mapKey = toMapKey(metadataKey, metadataVisibility);
                        removedEntries.add(mapKey);
                    }
                }
            }
            for (Map.Entry<String, Metadata.Entry> e : new ArrayList<>(entries.entrySet())) {
                if (e.getValue().getKey().equals(key)) {
                    entries.remove(e.getKey());
                }
            }
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        getEntriesLock().writeLock().lock();
        try {
            entries.clear();
            if (metadataEntries != null) {
                metadataEntries.clear();
            }
            metadataIndexes = new int[0];
            removedEntries.clear();
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    private void loadAll() {
        if (metadataEntries != null && metadataIndexes != null) {
            for (int metadataIndex : metadataIndexes) {
                MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                Visibility metadataVisibility = metadataEntry.getVisibility();
                String mapKey = toMapKey(metadataKey, metadataVisibility);
                if (removedEntries.contains(mapKey)) {
                    continue;
                }
                if (entries.containsKey(mapKey)) {
                    continue;
                }
                LazyEntry lazyEntry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                entries.put(mapKey, lazyEntry);
            }
        }
    }

    @Override
    public Collection<Metadata.Entry> entrySet() {
        getEntriesLock().readLock().lock();
        try {
            loadAll();
            return new ArrayList<>(entries.values());
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Metadata.Entry getEntry(String key, Visibility visibility) {
        String mapKey = toMapKey(key, visibility);

        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();

        if (removedEntries.contains(mapKey)) {
            return null;
        }

        try {
            Metadata.Entry entry = entries.get(toMapKey(key, visibility));
            if (entry != null) {
                return entry;
            }

            if (metadataEntries != null && metadataIndexes != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                    String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = metadataEntry.getVisibility();
                        if (metadataVisibility.equals(visibility)) {
                            LazyEntry lazyEntry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                            entries.put(mapKey, lazyEntry);
                            return lazyEntry;
                        }
                    }
                }
            }

            return null;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Metadata.Entry getEntry(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            Metadata.Entry entry = null;

            for (Map.Entry<String, Metadata.Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    if (entry != null) {
                        throw new VertexiumException("Multiple matching entries for key: " + key);
                    }
                    entry = e.getValue();
                }
            }

            if (metadataEntries != null && metadataIndexes != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                    String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = metadataEntry.getVisibility();
                        String mapKey = toMapKey(metadataKey, metadataVisibility);
                        if (entries.containsKey(mapKey)) {
                            continue;
                        }
                        if (entry != null) {
                            throw new VertexiumException("Multiple matching entries for key: " + key);
                        }
                        entry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                        entries.put(mapKey, entry);
                    }
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
            Map<String, Metadata.Entry> results = new HashMap<>();

            for (Map.Entry<String, Metadata.Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    String mapKey = toMapKey(e.getValue().getKey(), e.getValue().getVisibility());
                    results.put(mapKey, e.getValue());
                }
            }

            if (metadataEntries != null && metadataIndexes != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                    String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = metadataEntry.getVisibility();
                        String mapKey = toMapKey(metadataKey, metadataVisibility);
                        if (!results.containsKey(mapKey)) {
                            LazyEntry entry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                            entries.put(mapKey, entry);
                            results.put(mapKey, entry);
                        }
                    }
                }
            }

            return results.values();
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    private ReadWriteLock getEntriesLock() {
        // entriesLock may be null if this class has just been de-serialized
        if (entriesLock == null) {
            entriesLock = new ReentrantReadWriteLock();
        }
        return entriesLock;
    }

    private String toMapKey(String key, Visibility visibility) {
        return key + KEY_SEPARATOR + visibility.getVisibilityString();
    }

    private static class Entry implements Metadata.Entry {
        private final String key;
        private final Object value;
        private final Visibility visibility;

        public Entry(String key, Object value, Visibility visibility) {
            this.key = key;
            this.value = value;
            this.visibility = visibility;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }
    }

    private class LazyEntry implements Metadata.Entry {
        private final String key;
        private final Visibility visibility;
        private final MetadataEntry metadataEntry;
        private Object metadataValue;

        public LazyEntry(String key, Visibility visibility, MetadataEntry metadataEntry) {
            this.key = key;
            this.visibility = visibility;
            this.metadataEntry = metadataEntry;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            if (metadataValue == null) {
                metadataValue = metadataEntry.getValue(vertexiumSerializer);
                if (metadataValue == null) {
                    throw new VertexiumException("Invalid metadata value found.");
                }
            }
            return metadataValue;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }
    }
}
