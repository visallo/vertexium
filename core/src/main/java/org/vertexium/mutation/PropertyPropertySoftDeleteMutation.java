package org.vertexium.mutation;

import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.util.IncreasingTime;

public class PropertyPropertySoftDeleteMutation extends PropertySoftDeleteMutation {
    private final Property property;
    private final Object data;
    private final long timestamp;

    public PropertyPropertySoftDeleteMutation(Property property, Object data) {
        this.property = property;
        this.data = data;
        this.timestamp = IncreasingTime.currentTimeMillis();
    }

    @Override
    public String getKey() {
        return property.getKey();
    }

    @Override
    public String getName() {
        return property.getName();
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Visibility getVisibility() {
        return property.getVisibility();
    }

    public Property getProperty() {
        return property;
    }

    @Override
    public Object getData() {
        return data;
    }
}
