package org.vertexium.query;

import org.vertexium.Property;
import org.vertexium.PropertyDefinition;

import java.util.Collection;

public interface Predicate {
    boolean evaluate(Iterable<Property> properties, Object value, Collection<PropertyDefinition> propertyDefinitions);

    boolean evaluate(Object first, Object second, PropertyDefinition propertyDefinition);

    void validate(PropertyDefinition propertyDefinition);
}
