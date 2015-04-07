package org.neolumin.vertexium.mutation;

import org.neolumin.vertexium.*;
import org.neolumin.vertexium.property.MutablePropertyImpl;
import org.neolumin.vertexium.search.IndexHint;

import java.util.ArrayList;
import java.util.List;

import static org.neolumin.vertexium.util.Preconditions.checkNotNull;

public abstract class ExistingElementMutationImpl<T extends Element> implements ElementMutation<T>, ExistingElementMutation<T> {
    private final List<Property> properties = new ArrayList<>();
    private final List<PropertyRemoveMutation> propertyRemoves = new ArrayList<>();
    private Visibility newElementVisibility;
    private final List<AlterPropertyVisibility> alterPropertyVisibilities = new ArrayList<>();
    private final List<SetPropertyMetadata> setPropertyMetadatas = new ArrayList<>();
    private final T element;
    private IndexHint indexHint = IndexHint.INDEX;

    public ExistingElementMutationImpl(T element) {
        this.element = element;
    }

    public abstract T save(Authorizations authorizations);

    public ElementMutation<T> setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, new Metadata(), visibility);
    }

    public ElementMutation<T> setProperty(String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(DEFAULT_KEY, name, value, metadata, visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Visibility visibility) {
        return addPropertyValue(key, name, value, new Metadata(), visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(key, name, value, metadata, null, visibility);
    }

    @Override
    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Long timestamp, Visibility visibility) {
        checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        checkNotNull(value, "property value cannot be null for property: " + name + ":" + key);
        properties.add(new MutablePropertyImpl(key, name, value, metadata, timestamp, null, visibility));
        return this;
    }

    public Iterable<Property> getProperties() {
        return properties;
    }

    @Override
    public Iterable<PropertyRemoveMutation> getPropertyRemoves() {
        return propertyRemoves;
    }

    @Override
    public ElementMutation<T> removeProperty(Property property) {
        checkNotNull(property, "property cannot be null");
        propertyRemoves.add(new PropertyPropertyRemoveMutation(property));
        return this;
    }

    @Override
    public ExistingElementMutation<T> removeProperties(String name) {
        for (Property prop : this.element.getProperties(name)) {
            removeProperty(prop);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> removeProperties(String key, String name) {
        for (Property prop : this.element.getProperties(key, name)) {
            removeProperty(prop);
        }
        return this;
    }

    @Override
    public ElementMutation<T> removeProperty(String name, Visibility visibility) {
        Property property = this.element.getProperty(name, visibility);
        if (property != null) {
            removeProperty(property);
        }
        return this;
    }

    @Override
    public ElementMutation<T> removeProperty(String key, String name, Visibility visibility) {
        Property property = this.element.getProperty(key, name, visibility);
        if (property != null) {
            removeProperty(property);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(Property property, Visibility visibility) {
        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(property.getKey(), property.getName(), property.getVisibility(), visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(String name, Visibility visibility) {
        return alterPropertyVisibility(DEFAULT_KEY, name, visibility);
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(String key, String name, Visibility visibility) {
        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(key, name, null, visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterElementVisibility(Visibility visibility) {
        this.newElementVisibility = visibility;
        return this;
    }

    @Override
    public ExistingElementMutation<T> setPropertyMetadata(Property property, String metadataName, Object newValue, Visibility visibility) {
        this.setPropertyMetadatas.add(new SetPropertyMetadata(property.getKey(), property.getName(), property.getVisibility(), metadataName, newValue, visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> setPropertyMetadata(String propertyName, String metadataName, Object newValue, Visibility visibility) {
        return setPropertyMetadata(DEFAULT_KEY, propertyName, metadataName, newValue, visibility);
    }

    @Override
    public ExistingElementMutation<T> setPropertyMetadata(String propertyKey, String propertyName, String metadataName, Object newValue, Visibility visibility) {
        this.setPropertyMetadatas.add(new SetPropertyMetadata(propertyKey, propertyName, null, metadataName, newValue, visibility));
        return this;
    }

    @Override
    public T getElement() {
        return element;
    }

    public Visibility getNewElementVisibility() {
        return newElementVisibility;
    }

    public List<AlterPropertyVisibility> getAlterPropertyVisibilities() {
        return alterPropertyVisibilities;
    }

    public List<SetPropertyMetadata> getSetPropertyMetadatas() {
        return setPropertyMetadatas;
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
