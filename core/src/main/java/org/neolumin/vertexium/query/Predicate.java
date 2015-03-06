package org.neolumin.vertexium.query;

import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.PropertyDefinition;

import java.util.Map;

public interface Predicate {
    boolean evaluate(Iterable<Property> properties, Object value, Map<String, PropertyDefinition> propertyDefinitions);
}
