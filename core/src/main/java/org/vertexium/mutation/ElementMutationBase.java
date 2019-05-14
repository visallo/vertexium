package org.vertexium.mutation;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.search.IndexHint;
import org.vertexium.util.Preconditions;
import org.vertexium.util.StreamUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class ElementMutationBase<T extends Element, TResult extends ElementMutation<T>> implements ElementMutation<T> {
    private final List<Property> properties = new ArrayList<>();
    private final List<PropertyDeleteMutation> propertyDeletes = new ArrayList<>();
    private final List<PropertySoftDeleteMutation> propertySoftDeletes = new ArrayList<>();
    private final List<ExtendedDataMutation> extendedData = new ArrayList<>();
    private final List<ExtendedDataDeleteMutation> extendedDataDeletes = new ArrayList<>();
    private final List<AdditionalVisibilityAddMutation> additionalVisibilities = new ArrayList<>();
    private final List<AdditionalVisibilityDeleteMutation> additionalVisibilityDeletes = new ArrayList<>();
    private final List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities = new ArrayList<>();
    private final List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes = new ArrayList<>();
    private final List<MarkHiddenData> markHiddenData = new ArrayList<>();
    private final List<MarkVisibleData> markVisibleData = new ArrayList<>();
    private final List<DeleteExtendedDataRowData> deleteExtendedDataRowData = new ArrayList<>();
    private final List<MarkPropertyHiddenData> markPropertyHiddenData = new ArrayList<>();
    private final List<MarkPropertyVisibleData> markPropertyVisibleData = new ArrayList<>();
    private final List<AlterPropertyVisibility> alterPropertyVisibilities = new ArrayList<>();
    private final List<SetPropertyMetadata> setPropertyMetadata = new ArrayList<>();
    private boolean deleteElement;
    private SoftDeleteData softDeleteData;
    private Visibility newElementVisibility;
    private Object newElementVisibilityData;
    private IndexHint indexHint = IndexHint.INDEX;

    public TResult setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    public TResult setProperty(String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(DEFAULT_KEY, name, value, metadata, visibility);
    }

    public TResult addPropertyValue(String key, String name, Object value, Visibility visibility) {
        return addPropertyValue(key, name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    public TResult addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(key, name, value, metadata, null, visibility);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult addPropertyValue(String key, String name, Object value, Metadata metadata, Long timestamp, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        Preconditions.checkNotNull(value, "property value cannot be null for property: " + name + ":" + key);
        properties.add(new MutablePropertyImpl(key, name, value, metadata, timestamp, null, visibility, FetchHints.ALL_INCLUDING_HIDDEN));
        return (TResult) this;
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
        return extendedData;
    }

    @Override
    public Iterable<ExtendedDataDeleteMutation> getExtendedDataDeletes() {
        return extendedDataDeletes;
    }

    public Set<String> getAdditionalVisibilitiesAsStringSet() {
        Set<String> results = additionalVisibilities.stream()
            .map(AdditionalVisibilityAddMutation::getAdditionalVisibility)
            .collect(Collectors.toSet());
        results.removeAll(
            additionalVisibilityDeletes.stream()
                .map(AdditionalVisibilityDeleteMutation::getAdditionalVisibility)
                .collect(Collectors.toSet())
        );
        return results;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult deleteProperty(Property property) {
        if (!getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }

        Preconditions.checkNotNull(property, "property cannot be null");
        propertyDeletes.add(new PropertyPropertyDeleteMutation(property));
        return (TResult) this;
    }

    protected abstract FetchHints getFetchHints();

    @SuppressWarnings("unchecked")
    public TResult alterPropertyVisibility(Property property, Visibility visibility, Object eventData) {
        if (!getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }

        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(property.getKey(), property.getName(), property.getVisibility(), visibility, eventData));
        return (TResult) this;
    }

    public TResult alterPropertyVisibility(String name, Visibility visibility, Object eventData) {
        return alterPropertyVisibility(DEFAULT_KEY, name, visibility, eventData);
    }

    @SuppressWarnings("unchecked")
    public TResult alterPropertyVisibility(String key, String name, Visibility visibility, Object eventData) {
        if (!getFetchHints().isIncludePropertyAndMetadata(name)) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "Property " + name + " needs to be included with metadata");
        }

        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(key, name, null, visibility, eventData));
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    public TResult setPropertyMetadata(Property property, String metadataName, Object newValue, Visibility visibility) {
        this.setPropertyMetadata.add(new SetPropertyMetadata(property.getKey(), property.getName(), property.getVisibility(), metadataName, newValue, visibility));
        return (TResult) this;
    }

    public TResult setPropertyMetadata(String propertyName, String metadataName, Object newValue, Visibility visibility) {
        return setPropertyMetadata(DEFAULT_KEY, propertyName, metadataName, newValue, visibility);
    }

    @SuppressWarnings("unchecked")
    public TResult setPropertyMetadata(String propertyKey, String propertyName, String metadataName, Object newValue, Visibility visibility) {
        this.setPropertyMetadata.add(new SetPropertyMetadata(propertyKey, propertyName, null, metadataName, newValue, visibility));
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult addExtendedDataAdditionalVisibility(
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
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult deleteExtendedDataAdditionalVisibility(
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
        return (TResult) this;
    }

    @Override
    public TResult addExtendedData(String tableName, String row, String column, Object value, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, null, visibility);
    }

    @Override
    public TResult addExtendedData(String tableName, String row, String column, Object value, Long timestamp, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, timestamp, visibility);
    }

    @Override
    public TResult addExtendedData(String tableName, String row, String column, String key, Object value, Visibility visibility) {
        return addExtendedData(tableName, row, column, key, value, null, visibility);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult addExtendedData(String tableName, String row, String column, String key, Object value, Long timestamp, Visibility visibility) {
        this.extendedData.add(new ExtendedDataMutation(tableName, row, column, key, value, timestamp, visibility));
        return (TResult) this;
    }

    @Override
    public TResult deleteExtendedData(String tableName, String row, String column, Visibility visibility) {
        return deleteExtendedData(tableName, row, column, null, visibility);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult deleteExtendedData(String tableName, String row, String column, String key, Visibility visibility) {
        extendedDataDeletes.add(new ExtendedDataDeleteMutation(tableName, row, column, key, visibility));
        return (TResult) this;
    }

    @Override
    public List<AlterPropertyVisibility> getAlterPropertyVisibilities() {
        return alterPropertyVisibilities;
    }

    @Override
    public List<SetPropertyMetadata> getSetPropertyMetadata() {
        return setPropertyMetadata;
    }

    @Override
    public IndexHint getIndexHint() {
        return indexHint;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult addAdditionalVisibility(String visibility, Object eventData) {
        this.additionalVisibilities.add(new AdditionalVisibilityAddMutation(visibility, eventData));
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult deleteAdditionalVisibility(String visibility, Object eventData) {
        this.additionalVisibilityDeletes.add(new AdditionalVisibilityDeleteMutation(visibility, eventData));
        return (TResult) this;
    }

    @Override
    public Iterable<AdditionalVisibilityAddMutation> getAdditionalVisibilities() {
        return additionalVisibilities;
    }

    @Override
    public Iterable<AdditionalVisibilityDeleteMutation> getAdditionalVisibilityDeletes() {
        return additionalVisibilityDeletes;
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
    public List<MarkHiddenData> getMarkHiddenData() {
        return markHiddenData;
    }

    @Override
    public List<MarkVisibleData> getMarkVisibleData() {
        return markVisibleData;
    }

    @Override
    public List<MarkPropertyHiddenData> getMarkPropertyHiddenData() {
        return markPropertyHiddenData;
    }

    @Override
    public List<MarkPropertyVisibleData> getMarkPropertyVisibleData() {
        return markPropertyVisibleData;
    }

    @Override
    public List<DeleteExtendedDataRowData> getDeleteExtendedDataRowData() {
        return deleteExtendedDataRowData;
    }

    @Override
    public SoftDeleteData getSoftDeleteData() {
        return softDeleteData;
    }

    @Override
    public boolean isDeleteElement() {
        return deleteElement;
    }

    @Override
    public TResult deleteProperty(String name, Visibility visibility) {
        return deleteProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult deleteProperty(String key, String name, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        propertyDeletes.add(new KeyNameVisibilityPropertyDeleteMutation(key, name, visibility));
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult softDeleteProperty(Property property, Object eventData) {
        Preconditions.checkNotNull(property, "property cannot be null");
        propertySoftDeletes.add(new PropertyPropertySoftDeleteMutation(property, eventData));
        return (TResult) this;
    }

    @Override
    public TResult softDeleteProperty(String name, Visibility visibility, Object eventData) {
        return softDeleteProperty(ElementMutation.DEFAULT_KEY, name, visibility, eventData);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult softDeleteProperty(String key, String name, Visibility visibility, Object eventData) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        propertySoftDeletes.add(new KeyNameVisibilityPropertySoftDeleteMutation(key, name, visibility, eventData));
        return (TResult) this;
    }

    public ImmutableSet<String> getExtendedDataTableNames() {
        return extendedData.stream()
            .map(ExtendedDataMutation::getTableName)
            .collect(StreamUtils.toImmutableSet());
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult setIndexHint(IndexHint indexHint) {
        this.indexHint = indexHint;
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult deleteElement() {
        deleteElement = true;
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult softDeleteElement(Long timestamp, Object eventData) {
        softDeleteData = new SoftDeleteData(timestamp, eventData);
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult markElementHidden(Visibility visibility, Object eventData) {
        markHiddenData.add(new MarkHiddenData(visibility, eventData));
        return (TResult) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult markElementVisible(Visibility visibility, Object eventData) {
        markVisibleData.add(new MarkVisibleData(visibility, eventData));
        return (TResult) this;
    }

    @Override
    public ElementMutation<T> markPropertyHidden(
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object eventData
    ) {
        markPropertyHiddenData.add(new MarkPropertyHiddenData(
            key,
            name,
            propertyVisibility,
            timestamp,
            visibility,
            eventData
        ));
        return this;
    }

    @Override
    public ElementMutation<T> markPropertyVisible(
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object eventData
    ) {
        markPropertyVisibleData.add(new MarkPropertyVisibleData(
            key,
            name,
            propertyVisibility,
            timestamp,
            visibility,
            eventData
        ));
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TResult deleteExtendedDataRow(String tableName, String row) {
        deleteExtendedDataRowData.add(new DeleteExtendedDataRowData(tableName, row));
        return (TResult) this;
    }

    public Visibility getNewElementVisibility() {
        return newElementVisibility;
    }

    public Object getNewElementVisibilityData() {
        return newElementVisibilityData;
    }

    @SuppressWarnings("unchecked")
    public TResult alterElementVisibility(Visibility visibility, Object eventData) {
        this.newElementVisibility = visibility;
        this.newElementVisibilityData = eventData;
        return (TResult) this;
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

        if (alterPropertyVisibilities.size() > 0) {
            return true;
        }

        if (setPropertyMetadata.size() > 0) {
            return true;
        }

        if (extendedData.size() > 0) {
            return true;
        }

        if (extendedDataDeletes.size() > 0) {
            return true;
        }

        if (additionalVisibilities.size() > 0) {
            return true;
        }

        if (additionalVisibilityDeletes.size() > 0) {
            return true;
        }

        if (additionalExtendedDataVisibilities.size() > 0) {
            return true;
        }

        if (additionalExtendedDataVisibilityDeletes.size() > 0) {
            return true;
        }

        if (markHiddenData.size() > 0) {
            return true;
        }

        if (markVisibleData.size() > 0) {
            return true;
        }

        if (markPropertyHiddenData.size() > 0) {
            return true;
        }

        if (markPropertyVisibleData.size() > 0) {
            return true;
        }

        if (deleteElement || softDeleteData != null) {
            return true;
        }

        if (newElementVisibility != null) {
            return true;
        }

        return false;
    }

    public static class MarkHiddenData {
        private final Visibility visibility;
        private final Object eventData;

        public MarkHiddenData(Visibility visibility, Object eventData) {
            this.visibility = visibility;
            this.eventData = eventData;
        }

        public Visibility getVisibility() {
            return visibility;
        }

        public Object getEventData() {
            return eventData;
        }
    }

    public static class MarkVisibleData {
        private final Visibility visibility;
        private final Object eventData;

        public MarkVisibleData(Visibility visibility, Object eventData) {
            this.visibility = visibility;
            this.eventData = eventData;
        }

        public Visibility getVisibility() {
            return visibility;
        }

        public Object getEventData() {
            return eventData;
        }
    }

    public static class DeleteExtendedDataRowData {
        private final String tableName;
        private final String row;

        public DeleteExtendedDataRowData(String tableName, String row) {
            this.tableName = tableName;
            this.row = row;
        }

        public String getTableName() {
            return tableName;
        }

        public String getRow() {
            return row;
        }
    }

    public static class MarkPropertyHiddenData {
        private final String key;
        private final String name;
        private final Visibility propertyVisibility;
        private final Long timestamp;
        private final Visibility visibility;
        private final Object eventData;

        public MarkPropertyHiddenData(
            String key,
            String name,
            Visibility propertyVisibility,
            Long timestamp,
            Visibility visibility,
            Object eventData
        ) {
            this.key = key;
            this.name = name;
            this.propertyVisibility = propertyVisibility;
            this.timestamp = timestamp;
            this.visibility = visibility;
            this.eventData = eventData;
        }

        public String getKey() {
            return key;
        }

        public String getName() {
            return name;
        }

        public Visibility getPropertyVisibility() {
            return propertyVisibility;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public Visibility getVisibility() {
            return visibility;
        }

        public Object getEventData() {
            return eventData;
        }
    }

    public static class MarkPropertyVisibleData {
        private final String key;
        private final String name;
        private final Visibility propertyVisibility;
        private final Long timestamp;
        private final Visibility visibility;
        private final Object eventData;

        public MarkPropertyVisibleData(
            String key,
            String name,
            Visibility propertyVisibility,
            Long timestamp,
            Visibility visibility,
            Object eventData
        ) {
            this.key = key;
            this.name = name;
            this.propertyVisibility = propertyVisibility;
            this.timestamp = timestamp;
            this.visibility = visibility;
            this.eventData = eventData;
        }

        public String getKey() {
            return key;
        }

        public String getName() {
            return name;
        }

        public Visibility getPropertyVisibility() {
            return propertyVisibility;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public Visibility getVisibility() {
            return visibility;
        }

        public Object getEventData() {
            return eventData;
        }
    }

    public static class SoftDeleteData {
        private final Long timestamp;
        private final Object eventData;

        public SoftDeleteData(Long timestamp, Object eventData) {
            this.timestamp = timestamp;
            this.eventData = eventData;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public Object getEventData() {
            return eventData;
        }
    }
}
