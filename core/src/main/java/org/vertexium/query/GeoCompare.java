package org.vertexium.query;

import org.vertexium.Property;
import org.vertexium.PropertyDefinition;
import org.vertexium.type.GeoShape;

import java.util.Collection;

public enum GeoCompare implements Predicate {
    WITHIN;

    @Override
    public boolean evaluate(Iterable<Property> properties, Object second, Collection<PropertyDefinition> propertyDefinitions) {
        for (Property property : properties) {
            if (evaluate(property, second)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Property property, Object second) {
        switch (this) {
            case WITHIN:
                return ((GeoShape) second).within((GeoShape) property.getValue());
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }
}