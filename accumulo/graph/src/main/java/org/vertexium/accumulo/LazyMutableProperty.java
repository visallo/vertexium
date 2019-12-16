package org.vertexium.accumulo;

import org.vertexium.*;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.StreamingPropertyValueRef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class LazyMutableProperty extends MutableProperty {
    private final AccumuloGraph graph;
    private final VertexiumSerializer vertexiumSerializer;
    private final String propertyKey;
    private final String propertyName;
    private long timestamp;
    private final FetchHints fetchHints;
    private Set<Visibility> hiddenVisibilities;
    private byte[] propertyValue;
    private MetadataRef metadataRef;
    private Visibility visibility;
    private transient Object cachedPropertyValue;
    private transient Metadata cachedMetadata;

    public LazyMutableProperty(
        AccumuloGraph graph,
        VertexiumSerializer vertexiumSerializer,
        String propertyKey,
        String propertyName,
        byte[] propertyValue,
        MetadataRef metadataRef,
        Set<Visibility> hiddenVisibilities,
        Visibility visibility,
        long timestamp,
        FetchHints fetchHints
    ) {
        this.graph = graph;
        this.vertexiumSerializer = vertexiumSerializer;
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
        this.metadataRef = metadataRef;
        this.visibility = visibility;
        this.hiddenVisibilities = hiddenVisibilities;
        this.timestamp = timestamp;
        this.fetchHints = fetchHints;
    }

    @Override
    public void setValue(Object value) {
        this.cachedPropertyValue = value;
        this.propertyValue = null;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public void addHiddenVisibility(Visibility visibility) {
        if (hiddenVisibilities == null) {
            hiddenVisibilities = new HashSet<>();
        }
        hiddenVisibilities.add(visibility);
    }

    @Override
    public void removeHiddenVisibility(Visibility visibility) {
        if (hiddenVisibilities == null) {
            hiddenVisibilities = new HashSet<>();
        }
        hiddenVisibilities.remove(visibility);
    }

    @Override
    protected void updateMetadata(Property property) {
        this.cachedMetadata = null;
        if (property instanceof LazyMutableProperty) {
            this.metadataRef = ((LazyMutableProperty) property).metadataRef;
        } else {
            Collection<Metadata.Entry> entries = new ArrayList<>(property.getMetadata().entrySet());
            this.metadataRef = null;
            if (getFetchHints().isIncludePropertyAndMetadata(propertyName)) {
                for (Metadata.Entry metadataEntry : entries) {
                    getMetadata().add(metadataEntry.getKey(), metadataEntry.getValue(), metadataEntry.getVisibility());
                }
            }
        }
    }

    @Override
    public String getKey() {
        return this.propertyKey;
    }

    @Override
    public String getName() {
        return this.propertyName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getValue() {
        if (cachedPropertyValue == null) {
            if (propertyValue == null || propertyValue.length == 0) {
                return null;
            }
            cachedPropertyValue = this.vertexiumSerializer.bytesToObject(propertyValue);
            if (cachedPropertyValue instanceof StreamingPropertyValueRef) {
                cachedPropertyValue = ((StreamingPropertyValueRef) cachedPropertyValue).toStreamingPropertyValue(this.graph, getTimestamp());
            }
        }
        return cachedPropertyValue;
    }

    @Override
    public Visibility getVisibility() {
        return this.visibility;
    }

    @Override
    public Metadata getMetadata() {
        if (!fetchHints.isIncludePropertyMetadata()) {
            throw new VertexiumMissingFetchHintException(fetchHints, "includePropertyMetadata");
        }
        if (cachedMetadata == null) {
            if (metadataRef == null) {
                cachedMetadata = Metadata.create(fetchHints);
            } else {
                cachedMetadata = new LazyPropertyMetadata(
                    metadataRef.getMetadataEntries(),
                    metadataRef.getMetadataIndexes(),
                    getTimestamp(),
                    this.vertexiumSerializer,
                    graph.getNameSubstitutionStrategy(),
                    graph.getMetadataPlugin(),
                    fetchHints
                );
            }
        }
        return cachedMetadata;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        if (hiddenVisibilities == null) {
            return new ArrayList<>();
        }
        return hiddenVisibilities;
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        if (hiddenVisibilities != null) {
            for (Visibility v : getHiddenVisibilities()) {
                if (authorizations.canRead(v)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
}
