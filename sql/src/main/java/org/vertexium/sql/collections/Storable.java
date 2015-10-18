package org.vertexium.sql.collections;

import java.util.Map;

/**
 * This represents an object that can store itself to a persistent container.
 */
public interface Storable<T, U> {
    /**
     * Set the Map which represents this object's persistent container, as well as an optional context.
     */
    void setContainer(Map<String, T> map, U context);

    /**
     * Store the state of this object to its container.
     */
    void store();
}
