package org.vertexium.query;

import org.vertexium.Property;
import org.vertexium.PropertyDefinition;
import org.vertexium.VertexiumNotSupportedException;
import org.vertexium.type.GeoShape;
import org.vertexium.util.StreamUtils;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

public enum GeoCompare implements Predicate {
    INTERSECTS("intersects"),
    DISJOINT("disjoint"),
    WITHIN("within"),
    CONTAINS("contains");

    private final String compareName;

    GeoCompare(String compareName) {
        this.compareName = compareName;
    }

    public String getCompareName() {
        return compareName;
    }

    @Override
    public boolean evaluate(Iterable<Property> properties, Object second, Collection<PropertyDefinition> propertyDefinitions) {
        switch (this) {
            case WITHIN:
                AtomicBoolean hasProperty = new AtomicBoolean(false);
                boolean allMatch = StreamUtils.stream(properties).allMatch(property -> {
                    hasProperty.set(true);
                    return evaluate(property.getValue(), second);
                });
                return hasProperty.get() && allMatch;
            case INTERSECTS:
                return StreamUtils.stream(properties).anyMatch(property -> evaluate(property.getValue(), second));
            case DISJOINT:
                return StreamUtils.stream(properties).allMatch(property -> evaluate(property.getValue(), second));
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }

    @Override
    public boolean evaluate(Object first, Object second, PropertyDefinition propertyDefinition) {
        return evaluate(first, second);
    }

    @Override
    public void validate(PropertyDefinition propertyDefinition) {
        if (!GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
            throw new VertexiumNotSupportedException("GeoCompare predicates are not allowed for properties of type " + propertyDefinition.getDataType().getName());
        }
    }

    private boolean evaluate(Object testValue, Object second) {
        switch (this) {
            case WITHIN:
                return ((GeoShape) testValue).within((GeoShape) second);
            case INTERSECTS:
                return ((GeoShape) second).intersects((GeoShape) testValue);
            case DISJOINT:
                return !((GeoShape) second).intersects((GeoShape) testValue);
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }
}