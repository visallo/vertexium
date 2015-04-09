package org.vertexium.mutation;

import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertyPropertySoftDeleteMutation extends PropertySoftDeleteMutation {
    private final Property property;

    public PropertyPropertySoftDeleteMutation(Property property) {
        this.property = property;
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
    public Visibility getVisibility() {
        return property.getVisibility();
    }

    public Property getProperty() {
        return property;
    }
}
