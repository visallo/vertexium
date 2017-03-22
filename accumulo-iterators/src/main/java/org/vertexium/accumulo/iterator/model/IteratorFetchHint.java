package org.vertexium.accumulo.iterator.model;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public enum IteratorFetchHint {
    PROPERTIES,
    PROPERTY_METADATA,
    IN_EDGE_REFS,
    OUT_EDGE_REFS,
    INCLUDE_HIDDEN,
    IN_EDGE_LABELS,
    OUT_EDGE_LABELS,
    EXTENDED_DATA_TABLE_NAMES;

    public static final EnumSet<IteratorFetchHint> ALL = EnumSet.of(PROPERTIES, PROPERTY_METADATA, IN_EDGE_REFS, OUT_EDGE_REFS, EXTENDED_DATA_TABLE_NAMES);

    public static String toString(EnumSet<IteratorFetchHint> fetchHints) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (IteratorFetchHint fetchHint : fetchHints) {
            if (!first) {
                result.append(",");
            }
            result.append(fetchHint.name());
            first = false;
        }
        return result.toString();
    }

    public static EnumSet<IteratorFetchHint> parse(String fetchHintsString) {
        if (fetchHintsString == null) {
            throw new NullPointerException("fetchHintsString cannot be null");
        }
        String[] parts = fetchHintsString.split(",");
        List<IteratorFetchHint> results = new ArrayList<>();
        for (String part : parts) {
            String name = part.toUpperCase();
            if (name.trim().length() == 0) {
                continue;
            }
            try {
                results.add(IteratorFetchHint.valueOf(name));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Could not find enum value: '" + name + "'", ex);
            }
        }
        return create(results);
    }

    public static EnumSet<IteratorFetchHint> create(List<IteratorFetchHint> results) {
        if (results.size() == 0) {
            return EnumSet.noneOf(IteratorFetchHint.class);
        } else {
            return EnumSet.copyOf(results);
        }
    }
}
