package org.vertexium;

import org.vertexium.mutation.ExistingElementMutation;

/**
 * An element on the graph. This can be either a vertex or edge.
 * <p/>
 * Elements also contain properties. These properties are unique given their key, name, and visibility.
 * For example a property with key "key1" and name "age" could have to values, one with visibility "a" and one
 * with visibility "b".
 */
public interface Element {
    /**
     * id of the element.
     */
    String getId();

    /**
     * the visibility of the element.
     */
    Visibility getVisibility();

    /**
     * The timestamp of when this element was updated.
     */
    long getTimestamp();

    /**
     * an Iterable of all the properties on this element that you have access to based on the authorizations
     * used to retrieve the element.
     */
    Iterable<Property> getProperties();

    /**
     * Gets a property by key and name.
     *
     * @param key  the key of the property.
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String key, String name);

    /**
     * Gets a property by key, name, and visibility.
     *
     * @param key        the key of the property.
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String key, String name, Visibility visibility);

    /**
     * Gets property values from all timestamps in descending timestamp order.
     *
     * @param key        the key of the property.
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     */
    Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Authorizations authorizations);

    /**
     * Gets property values from the given range of timestamps in descending timestamp order.
     *
     * @param key        the key of the property.
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     */
    Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, final Long startTime, final Long endTime, Authorizations authorizations);

    /**
     * Gets a property by name, and visibility.
     *
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String name, Visibility visibility);

    /**
     * Gets a property by name. This assumes a single valued property. If multiple property values exists this will only return the first one.
     *
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String name);

    /**
     * an Iterable of all the properties with the given name on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param name The name of the property to retrieve
     */
    Iterable<Property> getProperties(String name);

    /**
     * an Iterable of all the properties with the given name and key on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param key  The property key
     * @param name The name of the property to retrieve
     */
    Iterable<Property> getProperties(String key, String name);

    /**
     * an Iterable of all the property values with the given name on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param name The name of the property to retrieve
     */
    Iterable<Object> getPropertyValues(String name);

    /**
     * an Iterable of all the property values with the given name and key on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param key  The property key
     * @param name The name of the property to retrieve
     */
    Iterable<Object> getPropertyValues(String key, String name);

    /**
     * Convenience method to retrieve the first value of the property with the given name. This method calls
     * Element#getPropertyValue(java.lang.String, int) with an index of 0.
     * <p/>
     * This method makes no attempt to verify that one and only one value exists given the name.
     *
     * @param name The name of the property to retrieve
     * @return The value of the property. null, if the property was not found.
     */
    Object getPropertyValue(String name);

    /**
     * Convenience method to retrieve the first value of the property with the given name. This method calls
     * Element#getPropertyValue(java.lang.String, java.lang.String, int) with an index of 0.
     * <p/>
     * This method makes no attempt to verify that one and only one value exists given the name.
     *
     * @param key  The key of the property
     * @param name The name of the property to retrieve
     * @return The value of the property. null, if the property was not found.
     */
    Object getPropertyValue(String key, String name);

    /**
     * Gets the nth property value of the named property. If the named property has multiple values this method
     * provides an easy way to get the value by index.
     * <p/>
     * This method is a convenience method and calls Element#getPropertyValues(java.lang.String)
     * and iterates over that list until the nth value.
     * <p/>
     * This method assumes the property values are retrieved in a deterministic order.
     *
     * @param name  The name of the property to retrieve.
     * @param index The zero based index into the values.
     * @return The value of the property. null, if the property doesn't exist or doesn't have that many values.
     */
    Object getPropertyValue(String name, int index);

    /**
     * Gets the nth property value of the named property. If the named property has multiple values this method
     * provides an easy way to get the value by index.
     * <p/>
     * This method is a convenience method and calls Element#getPropertyValues(java.lang.String, java.lang.String)
     * and iterates over that list until the nth value.
     * <p/>
     * This method assumes the property values are retrieved in a deterministic order.
     *
     * @param key   The property key
     * @param name  The name of the property to retrieve.
     * @param index The zero based index into the values.
     * @return The value of the property. null, if the property doesn't exist or doesn't have that many values.
     */
    Object getPropertyValue(String key, String name, int index);

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareVertex(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    <T extends Element> ExistingElementMutation<T> prepareMutation();

    /**
     * Permanently deletes a property given it's key and name from the element. Only properties which you have access
     * to can be deleted using this method.
     *
     * @param key  The property key.
     * @param name The property name.
     */
    void deleteProperty(String key, String name, Authorizations authorizations);

    /**
     * Permanently deletes all properties with the given name that you have access to. Only properties which you have
     * access to will be deleted.
     *
     * @param name The name of the property to delete.
     */
    void deleteProperties(String name, Authorizations authorizations);

    /**
     * Soft deletes a property given it's key and name from the element. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key  The property key.
     * @param name The property name.
     */
    void softDeleteProperty(String key, String name, Authorizations authorizations);

    /**
     * Soft deletes a property given it's key and name from the element for a given visibility. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key  The property key.
     * @param name The property name.
     * @param visibility The visibility string of the property to soft delete.
     */
    void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations);

    /**
     * Soft deletes all properties with the given name that you have access to. Only properties which you have
     * access to will be soft deleted.
     *
     * @param name The name of the property to delete.
     */
    void softDeleteProperties(String name, Authorizations authorizations);

    /**
     * Gets the graph that this element belongs to.
     */
    Graph getGraph();

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    void addPropertyValue(String key, String name, Object value, Visibility visibility, Authorizations authorizations);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    void addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations);

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
    void setProperty(String name, Object value, Visibility visibility, Authorizations authorizations);

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
    void setProperty(String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations);

    /**
     * Gets the authorizations used to get this element.
     */
    Authorizations getAuthorizations();

    /**
     * Merge the given element's properties into this.
     *
     * @param element The element to merge properties from.
     */
    void mergeProperties(Element element);

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is hidden.
     *                           This visibility can be a superset of the property visibility to mark
     *                           it as hidden for only a subset of authorizations.
     * @param authorizations     The authorizations used.
     */
    void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations);

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is hidden.
     *                       This visibility can be a superset of the property visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     */
    void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations);

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is now visible.
     * @param authorizations     The authorizations used.
     */
    void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations);

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is now visible.
     * @param authorizations The authorizations used.
     */
    void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations);

    /**
     * Given the supplied authorizations is this element hidden?
     *
     * @param authorizations the authorizations to check against.
     * @return true, if it would be hidden from those authorizations.
     */
    boolean isHidden(Authorizations authorizations);
}
