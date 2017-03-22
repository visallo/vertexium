package org.vertexium;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public enum FetchHint {
    PROPERTIES,
    PROPERTY_METADATA,
    IN_EDGE_REFS,
    OUT_EDGE_REFS,
    INCLUDE_HIDDEN,
    IN_EDGE_LABELS,
    OUT_EDGE_LABELS,
    EXTENDED_DATA_TABLE_NAMES;

    public static final EnumSet<FetchHint> NONE = EnumSet.noneOf(FetchHint.class);
    public static final EnumSet<FetchHint> DEFAULT = EnumSet.of(PROPERTIES, IN_EDGE_REFS, OUT_EDGE_REFS, EXTENDED_DATA_TABLE_NAMES);
    public static final EnumSet<FetchHint> ALL = EnumSet.of(PROPERTIES, PROPERTY_METADATA, IN_EDGE_REFS, OUT_EDGE_REFS, EXTENDED_DATA_TABLE_NAMES);
    public static final EnumSet<FetchHint> ALL_INCLUDING_HIDDEN = EnumSet.allOf(FetchHint.class);
    public static final EnumSet<FetchHint> EDGE_REFS = EnumSet.of(IN_EDGE_REFS, OUT_EDGE_REFS);
    public static final EnumSet<FetchHint> EDGE_LABELS = EnumSet.of(IN_EDGE_LABELS, OUT_EDGE_LABELS);

    public static String toString(EnumSet<FetchHint> fetchHints) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (FetchHint fetchHint : fetchHints) {
            if (!first) {
                result.append(",");
            }
            result.append(fetchHint.name());
            first = false;
        }
        return result.toString();
    }

    public static EnumSet<FetchHint> parse(String fetchHintsString) {
        if (fetchHintsString == null) {
            throw new NullPointerException("fetchHintsString cannot be null");
        }
        String[] parts = fetchHintsString.split(",");
        List<FetchHint> results = new ArrayList<>();
        for (String part : parts) {
            String name = part.toUpperCase();
            if (name.trim().length() == 0) {
                continue;
            }
            try {
                results.add(FetchHint.valueOf(name));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Could not find enum value: '" + name + "'", ex);
            }
        }
        return create(results);
    }

    public static EnumSet<FetchHint> create(List<FetchHint> results) {
        if (results.size() == 0) {
            return EnumSet.noneOf(FetchHint.class);
        } else {
            return EnumSet.copyOf(results);
        }
    }

    public static void checkFetchHints(EnumSet<FetchHint> fetchHints, FetchHint neededFetchHint) {
        checkFetchHints(fetchHints, EnumSet.of(neededFetchHint));
    }

    public static void checkFetchHints(EnumSet<FetchHint> fetchHints, EnumSet<FetchHint> neededFetchHints) {
        for (FetchHint neededFetchHint : neededFetchHints) {
            if (!fetchHints.contains(neededFetchHint)) {
                throw new VertexiumMissingFetchHintException(fetchHints, neededFetchHints);
            }
        }
    }
}
