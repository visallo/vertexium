package org.neolumin.vertexium.mutation;

import org.neolumin.vertexium.*;
import org.neolumin.vertexium.search.IndexHint;

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
     * Removes a property.
     *
     * @param property the property to remove.
     */
    ElementMutation<T> removeProperty(Property property);

    /**
     * Removes the default property with that name.
     *
     * @param name       the property name to remove.
     * @param visibility the visibility of the property to remove.
     */
    ElementMutation<T> removeProperty(String name, Visibility visibility);

    /**
     * Removes a property.
     *
     * @param key        the key of the property to remove.
     * @param name       the name of the property to remove.
     * @param visibility the visibility of the property to remove.
     */
    ElementMutation<T> removeProperty(String key, String name, Visibility visibility);

    /**
     * Gets the properties currently in this mutation.
     */
    Iterable<Property> getProperties();

    /**
     * Gets the properties currently being removed in this mutation.
     */
    Iterable<PropertyRemoveMutation> getPropertyRemoves();

    /**
     * Sets the index hint of this element.
     */
    ElementMutation<T> setIndexHint(IndexHint indexHint);
}
