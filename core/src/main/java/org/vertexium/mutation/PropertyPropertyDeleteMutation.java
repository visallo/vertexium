package org.vertexium.mutation;

import org.vertexium.Property;
import org.vertexium.Visibility;

public class PropertyPropertyDeleteMutation extends PropertyDeleteMutation {
    private final Property property;

    public PropertyPropertyDeleteMutation(Property property) {
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
