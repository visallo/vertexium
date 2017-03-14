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
            if (evaluate(property.getValue(), second)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean evaluate(Object first, Object second, PropertyDefinition propertyDefinition) {
        return evaluate(first, second);
    }

    private boolean evaluate(Object testValue, Object second) {
        switch (this) {
            case WITHIN:
                return ((GeoShape) second).within((GeoShape) testValue);
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }
}