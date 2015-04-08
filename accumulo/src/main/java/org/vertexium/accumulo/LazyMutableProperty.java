package org.vertexium.accumulo;

import org.vertexium.Authorizations;
import org.vertexium.Metadata;
import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.accumulo.serializer.ValueSerializer;
import org.vertexium.property.MutableProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class LazyMutableProperty extends MutableProperty {
    private final AccumuloGraph graph;
    private final ValueSerializer valueSerializer;
    private final String propertyKey;
    private final String propertyName;
    private long timestamp;
    private Set<Visibility> hiddenVisibilities;
    private byte[] propertyValue;
    private LazyPropertyMetadata metadata;
    private Visibility visibility;
    private transient Object cachedPropertyValue;
    private transient Metadata cachedMetadata;

    public LazyMutableProperty(
            AccumuloGraph graph,
            ValueSerializer valueSerializer,
            String propertyKey,
            String propertyName,
            byte[] propertyValue,
            LazyPropertyMetadata metadata,
            Set<Visibility> hiddenVisibilities,
            Visibility visibility,
            long timestamp
    ) {
        this.graph = graph;
        this.valueSerializer = valueSerializer;
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
        this.metadata = metadata;
        this.visibility = visibility;
        this.hiddenVisibilities = hiddenVisibilities;
        this.timestamp = timestamp;
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
            this.metadata = ((LazyMutableProperty) property).metadata;
        } else {
            Collection<Metadata.Entry> entries = new ArrayList<>(property.getMetadata().entrySet());
            this.metadata = new LazyPropertyMetadata();
            for (Metadata.Entry metadataEntry : entries) {
                getMetadata().add(metadataEntry.getKey(), metadataEntry.getValue(), metadataEntry.getVisibility());
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
    public Object getValue() {
        if (cachedPropertyValue == null) {
            if (propertyValue == null || propertyValue.length == 0) {
                return null;
            }
            cachedPropertyValue = this.valueSerializer.valueToObject(propertyValue);
            if (cachedPropertyValue instanceof StreamingPropertyValueRef) {
                cachedPropertyValue = ((StreamingPropertyValueRef) cachedPropertyValue).toStreamingPropertyValue(this.graph);
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
        if (cachedMetadata == null) {
            if (metadata == null) {
                cachedMetadata = new Metadata();
            } else {
                cachedMetadata = metadata.toMetadata(this.valueSerializer);
            }
        }
        return cachedMetadata;
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
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
