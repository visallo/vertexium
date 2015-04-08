package org.vertexium.query;

import org.vertexium.Property;
import org.vertexium.PropertyDefinition;

import java.util.Map;

public interface Predicate {
    boolean evaluate(Iterable<Property> properties, Object value, Map<String, PropertyDefinition> propertyDefinitions);
}
