package org.vertexium.mutation;

import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertyPropertySoftDeleteMutation extends PropertySoftDeleteMutation {
    private final Property property;
    private final Object data;

    public PropertyPropertySoftDeleteMutation(Property property, Object data) {
        this.property = property;
        this.data = data;
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
        return property.getTimestamp();
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
