package org.vertexium;

import java.util.EnumSet;

public enum FetchHint {
    PROPERTIES,
    PROPERTY_METADATA,
    IN_EDGE_REFS,
    OUT_EDGE_REFS,
    INCLUDE_HIDDEN;

    public static final EnumSet<FetchHint> NONE = EnumSet.noneOf(FetchHint.class);
    public static final EnumSet<FetchHint> ALL = EnumSet.of(PROPERTIES, PROPERTY_METADATA, IN_EDGE_REFS, OUT_EDGE_REFS);
    public static final EnumSet<FetchHint> ALL_INCLUDING_HIDDEN = EnumSet.allOf(FetchHint.class);
    public static final EnumSet<FetchHint> EDGE_REFS = EnumSet.of(IN_EDGE_REFS, OUT_EDGE_REFS);

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
}
