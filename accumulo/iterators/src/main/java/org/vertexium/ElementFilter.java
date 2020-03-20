package org.vertexium;

import java.util.EnumSet;

/**
 * This is a copy of core/ElementFilter so the iterator can use it
 */
public enum ElementFilter {
    ELEMENT,
    PROPERTY,
    PROPERTY_METADATA;

    public static EnumSet<ElementFilter> ALL = EnumSet.allOf(ElementFilter.class);
}
