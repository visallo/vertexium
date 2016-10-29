package org.vertexium.mutation;

import org.vertexium.*;
import org.vertexium.search.IndexHint;

public interface ElementMutation<T extends Element> {
    public static final String DEFAULT_KEY = "";

    /**
     * saves the properties to the graph.
     *
     * @return the element which was mutated.
     */
    T save(Authorizations authorizations);

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> setProperty(String name, Object value, Visibility visibility);

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> setProperty(String name, Object value, Metadata metadata, Visibility visibility);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> addPropertyValue(String key, String name, Object value, Visibility visibility);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param timestamp  The timestamp of the property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Long timestamp, Visibility visibility);

    /**
     * Deletes a property.
     *
     * @param property the property to delete.
     */
    ElementMutation<T> deleteProperty(Property property);

    /**
     * Soft deletes a property.
     *
     * @param property the property to soft delete.
     */
    ElementMutation<T> softDeleteProperty(Property property);

    /**
     * Deletes the default property with that name.
     *
     * @param name       the property name to delete.
     * @param visibility the visibility of the property to delete.
     */
    ElementMutation<T> deleteProperty(String name, Visibility visibility);

    /**
     * Soft deletes the default property with that name.
     *
     * @param name       the property name to soft delete.
     * @param visibility the visibility of the property to soft delete.
     */
    ElementMutation<T> softDeleteProperty(String name, Visibility visibility);

    /**
     * Deletes a property.
     *
     * @param key        the key of the property to delete.
     * @param name       the name of the property to delete.
     * @param visibility the visibility of the property to delete.
     */
    ElementMutation<T> deleteProperty(String key, String name, Visibility visibility);

    /**
     * Soft deletes a property.
     *
     * @param key        the key of the property to soft delete.
     * @param name       the name of the property to soft delete.
     * @param visibility the visibility of the property to soft delete.
     */
    ElementMutation<T> softDeleteProperty(String key, String name, Visibility visibility);

    /**
     * Gets the properties currently in this mutation.
     */
    Iterable<Property> getProperties();

    /**
     * Gets the properties currently being deleted in this mutation.
     */
    Iterable<PropertyDeleteMutation> getPropertyDeletes();

    /**
     * Gets the properties currently being soft deleted in this mutation.
     */
    Iterable<PropertySoftDeleteMutation> getPropertySoftDeletes();

    /**
     * Sets the index hint of this element.
     */
    ElementMutation<T> setIndexHint(IndexHint indexHint);

    /**
     * Gets the currently set index hint.
     */
    IndexHint getIndexHint();
}
