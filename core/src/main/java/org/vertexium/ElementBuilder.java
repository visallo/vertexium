package org.vertexium;

import org.vertexium.mutation.*;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.search.IndexHint;
import org.vertexium.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

public abstract class ElementBuilder<T extends Element> implements ElementMutation<T> {
    private final List<Property> properties = new ArrayList<>();
    private final List<PropertyDeleteMutation> propertyDeletes = new ArrayList<>();
    private final List<PropertySoftDeleteMutation> propertySoftDeletes = new ArrayList<>();
    private IndexHint indexHint = IndexHint.INDEX;

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     * 
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, new Metadata(), visibility);
    }

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> setProperty(String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(ElementMutation.DEFAULT_KEY, name, value, metadata, visibility);
    }

    /**
     * Adds or updates a property.
     *
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> addPropertyValue(String key, String name, Object value, Visibility visibility) {
        return addPropertyValue(key, name, value, new Metadata(), visibility);
    }

    /**
     * Adds or updates a property.
     *
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(key, name, value, metadata, null, visibility);
    }

    @Override
    public ElementBuilder<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Long timestamp, Visibility visibility) {
        if (name == null) {
            throw new NullPointerException("property name cannot be null for property: " + name + ":" + key);
        }
        if (value == null) {
            throw new NullPointerException("property value cannot be null for property: " + name + ":" + key);
        }
        this.properties.add(new MutablePropertyImpl(key, name, value, metadata, timestamp, null, visibility));
        return this;
    }

    @Override
    public ElementBuilder<T> deleteProperty(Property property) {
        propertyDeletes.add(new PropertyPropertyDeleteMutation(property));
        return this;
    }

    @Override
    public ElementBuilder<T> deleteProperty(String name, Visibility visibility) {
        return deleteProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public ElementBuilder<T> deleteProperty(String key, String name, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        propertyDeletes.add(new KeyNameVisibilityPropertyDeleteMutation(key, name, visibility));
        return this;
    }

    @Override
    public ElementBuilder<T> softDeleteProperty(Property property) {
        propertySoftDeletes.add(new PropertyPropertySoftDeleteMutation(property));
        return this;
    }

    @Override
    public ElementBuilder<T> softDeleteProperty(String name, Visibility visibility) {
        return softDeleteProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public ElementBuilder<T> softDeleteProperty(String key, String name, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        propertySoftDeletes.add(new KeyNameVisibilityPropertySoftDeleteMutation(key, name, visibility));
        return this;
    }

    /**
     * saves the element to the graph.
     *
     * @return either the vertex or edge just saved.
     */
    public abstract T save(Authorizations authorizations);

    public Iterable<Property> getProperties() {
        return properties;
    }

    public Iterable<PropertyDeleteMutation> getPropertyDeletes() {
        return propertyDeletes;
    }

    public Iterable<PropertySoftDeleteMutation> getPropertySoftDeletes() {
        return propertySoftDeletes;
    }

    public IndexHint getIndexHint() {
        return indexHint;
    }

    @Override
    public ElementMutation<T> setIndexHint(IndexHint indexHint) {
        this.indexHint = indexHint;
        return this;
    }
}
