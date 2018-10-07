package org.vertexium;

import java.util.ArrayList;
import java.util.Collection;

public interface Metadata {
    static Metadata create(FetchHints fetchHints) {
        return new MapMetadata(fetchHints);
    }

    static Metadata create() {
        return new MapMetadata();
    }

    static Metadata create(Metadata metadata) {
        return new MapMetadata(metadata);
    }

    static Metadata create(Metadata metadata, FetchHints fetchHints) {
        return new MapMetadata(metadata, fetchHints);
    }

    Metadata add(String key, Object value, Visibility visibility);

    void remove(String key, Visibility visibility);

    void clear();

    void remove(String key);

    Collection<Entry> entrySet();

    Entry getEntry(String key, Visibility visibility);

    Entry getEntry(String key);

    Collection<Entry> getEntries(String key);

    FetchHints getFetchHints();

    default Object getValue(String key, Visibility visibility) {
        Entry entry = getEntry(key, visibility);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    default Object getValue(String key) {
        Entry entry = getEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    default Collection<Object> getValues(String key) {
        Collection<Object> results = new ArrayList<>();
        Collection<Metadata.Entry> entries = getEntries(key);
        for (Metadata.Entry entry : entries) {
            results.add(entry.getValue());
        }
        return results;
    }

    default boolean containsKey(String key) {
        return getEntries(key).size() > 0;
    }

    default boolean contains(String key, Visibility visibility) {
        return getEntry(key, visibility) != null;
    }

    interface Entry {
        String getKey();

        Object getValue();

        Visibility getVisibility();
    }
}
