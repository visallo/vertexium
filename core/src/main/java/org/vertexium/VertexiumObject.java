package org.vertexium;

import com.google.common.collect.ImmutableSet;
import org.vertexium.util.ConvertingIterable;

import java.util.Iterator;
import java.util.Set;

public interface VertexiumObject extends Comparable {
    /**
     * Id of the object
     */
    Object getId();

    /**
     * an Iterable of all the properties on this element that you have access to based on the authorizations
     * used to retrieve the element.
     */
    Iterable<Property> getProperties();

    /**
     * Set of all additional visibilities on this object
     */
    ImmutableSet<String> getAdditionalVisibilities();

    /**
     * Gets a property by name. This assumes a single valued property. If multiple property values exists this will only return the first one.
     *
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    default Property getProperty(String name) {
        Iterator<Property> propertiesWithName = getProperties(name).iterator();
        if (propertiesWithName.hasNext()) {
            return propertiesWithName.next();
        }
        return null;
    }

    /**
     * Convenience method to retrieve the first value of the property with the given name. This method calls
     * Element#getPropertyValue(java.lang.String, int) with an index of 0.
     * <p/>
     * This method makes no attempt to verify that one and only one value exists given the name.
     *
     * @param name The name of the property to retrieve
     * @return The value of the property. null, if the property was not found.
     */
    default Object getPropertyValue(String name) {
        return getPropertyValue(name, 0);
    }

    /**
     * Gets a property by key and name.
     *
     * @param key  the key of the property.
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    default Property getProperty(String key, String name) {
        return getProperty(key, name, null);
    }

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
     * Gets a property by name, and visibility.
     *
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String name, Visibility visibility);

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
    default Iterable<Object> getPropertyValues(String name) {
        return new ConvertingIterable<Property, Object>(getProperties(name)) {
            @Override
            protected Object convert(Property o) {
                return o.getValue();
            }
        };
    }

    /**
     * an Iterable of all the property values with the given name and key on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param key  The property key
     * @param name The name of the property to retrieve
     */
    default Iterable<Object> getPropertyValues(String key, String name) {
        return new ConvertingIterable<Property, Object>(getProperties(key, name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

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
    default Object getPropertyValue(String key, String name) {
        return getPropertyValue(key, name, 0);
    }

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
    default Object getPropertyValue(String name, int index) {
        Iterator<Object> values = getPropertyValues(name).iterator();
        while (values.hasNext() && index >= 0) {
            Object v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }

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
    default Object getPropertyValue(String key, String name, int index) {
        Iterator<Object> values = getPropertyValues(key, name).iterator();
        while (values.hasNext() && index >= 0) {
            Object v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }
}
