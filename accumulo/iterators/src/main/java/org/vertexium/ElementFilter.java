package org.vertexium;

import java.util.EnumSet;

public enum ElementFilter {
    ELEMENT,
    PROPERTY,
    PROPERTY_METADATA;

    public static EnumSet<ElementFilter> ALL = EnumSet.allOf(ElementFilter.class);
}
