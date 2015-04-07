package org.neolumin.vertexium;

import org.neolumin.vertexium.mutation.ElementMutation;
import org.neolumin.vertexium.mutation.KeyNameVisibilityPropertyRemoveMutation;
import org.neolumin.vertexium.mutation.PropertyPropertyRemoveMutation;
import org.neolumin.vertexium.mutation.PropertyRemoveMutation;
import org.neolumin.vertexium.property.MutablePropertyImpl;
import org.neolumin.vertexium.search.IndexHint;

import java.util.ArrayList;
import java.util.List;

import static org.neolumin.vertexium.util.Preconditions.checkNotNull;

public abstract class ElementBuilder<T extends Element> implements ElementMutation<T> {
    private final List<Property> properties = new ArrayList<>();
    private final List<PropertyRemoveMutation> propertyRemoves = new ArrayList<>();
    private IndexHint indexHint = IndexHint.INDEX;

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     * <p/>
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
     * <p/>
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
     * <p/>
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
     * <p/>
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
        checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        checkNotNull(value, "property value cannot be null for property: " + name + ":" + key);
        this.properties.add(new MutablePropertyImpl(key, name, value, metadata, timestamp, null, visibility));
        return this;
    }

    public ElementBuilder<T> removeProperty(Property property) {
        propertyRemoves.add(new PropertyPropertyRemoveMutation(property));
        return this;
    }

    public ElementBuilder<T> removeProperty(String name, Visibility visibility) {
        return removeProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    public ElementBuilder<T> removeProperty(String key, String name, Visibility visibility) {
        checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        propertyRemoves.add(new KeyNameVisibilityPropertyRemoveMutation(key, name, visibility));
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

    public Iterable<PropertyRemoveMutation> getPropertyRemoves() {
        return propertyRemoves;
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
