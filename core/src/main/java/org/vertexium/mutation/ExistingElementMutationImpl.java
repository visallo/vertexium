package org.vertexium.mutation;

import org.vertexium.*;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.search.IndexHint;
import org.vertexium.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

public abstract class ExistingElementMutationImpl<T extends Element> implements ElementMutation<T>, ExistingElementMutation<T> {
    private final List<Property> properties = new ArrayList<>();
    private final List<PropertyDeleteMutation> propertyDeletes = new ArrayList<>();
    private final List<PropertySoftDeleteMutation> propertySoftDeletes = new ArrayList<>();
    private final List<AlterPropertyVisibility> alterPropertyVisibilities = new ArrayList<>();
    private final List<SetPropertyMetadata> setPropertyMetadatas = new ArrayList<>();
    private final List<ExtendedDataMutation> extendedDatas = new ArrayList<>();
    private final List<ExtendedDataDeleteMutation> extendedDataDeletes = new ArrayList<>();
    private final List<AdditionalVisibilityAddMutation> additionalVisibilityAddMutations = new ArrayList<>();
    private final List<AdditionalVisibilityDeleteMutation> additionalVisibilityDeleteMutations = new ArrayList<>();
    private final List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities = new ArrayList<>();
    private final List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes = new ArrayList<>();
    private final T element;
    private Visibility newElementVisibility;
    private Object newElementVisibilityData;
    private Visibility oldElementVisibility;
    private IndexHint indexHint = IndexHint.INDEX;

    public ExistingElementMutationImpl(T element) {
        this.element = element;
        if (element != null) {
            this.oldElementVisibility = element.getVisibility();
        }
    }

    @Override
    public ElementType getElementType() {
        return ElementType.getTypeFromElement(getElement());
    }

    @Override
    public String getId() {
        return getElement().getId();
    }

    @Override
    public Visibility getVisibility() {
        return getElement().getVisibility();
    }

    public abstract T save(Authorizations authorizations);

    public ElementMutation<T> setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    public ElementMutation<T> setProperty(String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(DEFAULT_KEY, name, value, metadata, visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Visibility visibility) {
        return addPropertyValue(key, name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(key, name, value, metadata, null, visibility);
    }

    @Override
    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Metadata metadata, Long timestamp, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        Preconditions.checkNotNull(value, "property value cannot be null for property: " + name + ":" + key);
        properties.add(new MutablePropertyImpl(key, name, value, metadata, timestamp, null, visibility, FetchHints.ALL_INCLUDING_HIDDEN));
        return this;
    }

    public Iterable<Property> getProperties() {
        return properties;
    }

    @Override
    public Iterable<PropertyDeleteMutation> getPropertyDeletes() {
        return propertyDeletes;
    }

    @Override
    public Iterable<PropertySoftDeleteMutation> getPropertySoftDeletes() {
        return propertySoftDeletes;
    }

    @Override
    public Iterable<ExtendedDataMutation> getExtendedData() {
        return extendedDatas;
    }

    @Override
    public ElementMutation<T> deleteProperty(Property property) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new VertexiumMissingFetchHintException(element.getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }

        Preconditions.checkNotNull(property, "property cannot be null");
        propertyDeletes.add(new PropertyPropertyDeleteMutation(property));
        return this;
    }

    @Override
    public Iterable<ExtendedDataDeleteMutation> getExtendedDataDeletes() {
        return extendedDataDeletes;
    }

    @Override
    public ExistingElementMutation<T> deleteProperties(String name) {
        for (Property prop : this.element.getProperties(name)) {
            deleteProperty(prop);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> deleteProperties(String key, String name) {
        for (Property prop : this.element.getProperties(key, name)) {
            deleteProperty(prop);
        }
        return this;
    }

    @Override
    public ElementMutation<T> deleteProperty(String name, Visibility visibility) {
        Property property = this.element.getProperty(name, visibility);
        if (property != null) {
            deleteProperty(property);
        }
        return this;
    }

    @Override
    public ElementMutation<T> deleteProperty(String key, String name, Visibility visibility) {
        Property property = this.element.getProperty(key, name, visibility);
        if (property != null) {
            deleteProperty(property);
        }
        return this;
    }

    @Override
    public ElementMutation<T> softDeleteProperty(Property property, Object eventData) {
        Preconditions.checkNotNull(property, "property cannot be null");
        propertySoftDeletes.add(new PropertyPropertySoftDeleteMutation(property, eventData));
        return this;
    }

    @Override
    public ExistingElementMutation<T> softDeleteProperties(String name, Object eventData) {
        for (Property prop : this.element.getProperties(name)) {
            softDeleteProperty(prop, eventData);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> softDeleteProperties(String key, String name, Object eventData) {
        for (Property prop : this.element.getProperties(key, name)) {
            softDeleteProperty(prop, eventData);
        }
        return this;
    }

    @Override
    public ElementMutation<T> softDeleteProperty(String name, Visibility visibility, Object eventData) {
        Property property = this.element.getProperty(name, visibility);
        if (property != null) {
            softDeleteProperty(property, eventData);
        }
        return this;
    }

    @Override
    public ElementMutation<T> softDeleteProperty(String key, String name, Visibility visibility, Object eventData) {
        Property property = this.element.getProperty(key, name, visibility);
        if (property != null) {
            softDeleteProperty(property, eventData);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(Property property, Visibility visibility, Object eventData) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new VertexiumMissingFetchHintException(element.getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }

        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(property.getKey(), property.getName(), property.getVisibility(), visibility, eventData));
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(String name, Visibility visibility, Object eventData) {
        return alterPropertyVisibility(DEFAULT_KEY, name, visibility, eventData);
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(String key, String name, Visibility visibility, Object eventData) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(name)) {
            throw new VertexiumMissingFetchHintException(element.getFetchHints(), "Property " + name + " needs to be included with metadata");
        }

        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(key, name, null, visibility, eventData));
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterElementVisibility(Visibility visibility, Object eventData) {
        this.newElementVisibility = visibility;
        this.newElementVisibilityData = eventData;
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
    public ElementMutation<T> addAdditionalVisibility(String visibility, Object eventData) {
        this.additionalVisibilityAddMutations.add(new AdditionalVisibilityAddMutation(visibility, eventData));
        return this;
    }

    @Override
    public ElementMutation<T> deleteAdditionalVisibility(String visibility, Object eventData) {
        this.additionalVisibilityDeleteMutations.add(new AdditionalVisibilityDeleteMutation(visibility, eventData));
        return this;
    }

    @Override
    public ElementMutation<T> addExtendedDataAdditionalVisibility(
        String tableName,
        String row,
        String additionalVisibility,
        Object eventData
    ) {
        this.additionalExtendedDataVisibilities.add(new AdditionalExtendedDataVisibilityAddMutation(
            tableName,
            row,
            additionalVisibility,
            eventData
        ));
        return this;
    }

    @Override
    public ElementMutation<T> deleteExtendedDataAdditionalVisibility(
        String tableName,
        String row,
        String additionalVisibility,
        Object eventData
    ) {
        this.additionalExtendedDataVisibilityDeletes.add(new AdditionalExtendedDataVisibilityDeleteMutation(
            tableName,
            row,
            additionalVisibility,
            eventData
        ));
        return this;
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, Object value, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, null, visibility);
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, Object value, Long timestamp, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, timestamp, visibility);
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, String key, Object value, Visibility visibility) {
        return addExtendedData(tableName, row, column, key, value, null, visibility);
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, String key, Object value, Long timestamp, Visibility visibility) {
        this.extendedDatas.add(new ExtendedDataMutation(tableName, row, column, key, value, timestamp, visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> deleteExtendedData(String tableName, String row, String column, Visibility visibility) {
        return deleteExtendedData(tableName, row, column, null, visibility);
    }

    @Override
    public ExistingElementMutation<T> deleteExtendedData(String tableName, String row, String column, String key, Visibility visibility) {
        extendedDataDeletes.add(new ExtendedDataDeleteMutation(tableName, row, column, key, visibility));
        return this;
    }

    @Override
    public T getElement() {
        return element;
    }

    @Override
    public Visibility getNewElementVisibility() {
        return newElementVisibility;
    }

    @Override
    public Object getNewElementVisibilityData() {
        return newElementVisibilityData;
    }

    @Override
    public Visibility getOldElementVisibility() {
        return oldElementVisibility;
    }

    @Override
    public List<AlterPropertyVisibility> getAlterPropertyVisibilities() {
        return alterPropertyVisibilities;
    }

    @Override
    public List<SetPropertyMetadata> getSetPropertyMetadatas() {
        return setPropertyMetadatas;
    }

    @Override
    public IndexHint getIndexHint() {
        return indexHint;
    }

    @Override
    public Iterable<AdditionalVisibilityAddMutation> getAdditionalVisibilities() {
        return additionalVisibilityAddMutations;
    }

    @Override
    public Iterable<AdditionalVisibilityDeleteMutation> getAdditionalVisibilityDeletes() {
        return additionalVisibilityDeleteMutations;
    }

    @Override
    public List<AdditionalExtendedDataVisibilityAddMutation> getAdditionalExtendedDataVisibilities() {
        return additionalExtendedDataVisibilities;
    }

    @Override
    public List<AdditionalExtendedDataVisibilityDeleteMutation> getAdditionalExtendedDataVisibilityDeletes() {
        return additionalExtendedDataVisibilityDeletes;
    }

    @Override
    public ElementMutation<T> setIndexHint(IndexHint indexHint) {
        this.indexHint = indexHint;
        return this;
    }

    @Override
    public boolean hasChanges() {
        if (properties.size() > 0) {
            return true;
        }

        if (propertyDeletes.size() > 0) {
            return true;
        }

        if (propertySoftDeletes.size() > 0) {
            return true;
        }

        if (newElementVisibility != null) {
            return true;
        }

        if (alterPropertyVisibilities.size() > 0) {
            return true;
        }

        if (setPropertyMetadatas.size() > 0) {
            return true;
        }

        if (extendedDatas.size() > 0) {
            return true;
        }

        if (extendedDataDeletes.size() > 0) {
            return true;
        }

        return false;
    }
}
