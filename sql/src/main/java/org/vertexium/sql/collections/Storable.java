package org.vertexium.sql.collections;

import java.util.Map;

/**
 * This represents an object that can store itself to a persistent container.
 */
public interface Storable<T> {
    /**
     * Set the Map which represents this object's persistent container.
     */
    void setContainer(Map<String, T> map);

    /**
     * Store the state of this object to its container.
     */
    void store();
}
