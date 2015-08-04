package org.vertexium;

import java.util.EnumSet;

public enum EdgeFilter {
    EDGE,
    PROPERTY,
    PROPERTY_METADATA;

    public static EnumSet<EdgeFilter> ALL = EnumSet.allOf(EdgeFilter.class);
}
