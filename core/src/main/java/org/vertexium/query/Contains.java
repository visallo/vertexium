package org.vertexium.query;

import org.vertexium.Property;
import org.vertexium.PropertyDefinition;
import org.vertexium.VertexiumException;

import java.util.Map;

public enum Contains implements Predicate {
    IN, NOT_IN;

    @Override
    public boolean evaluate(Iterable<Property> properties, Object second, Map<String, PropertyDefinition> propertyDefinitions) {
        for (Property property : properties) {
            PropertyDefinition propertyDefinition = propertyDefinitions.get(property.getName());
            if (evaluate(property, second, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Property property, Object second, PropertyDefinition propertyDefinition) {
        if (second instanceof Iterable) {
            switch (this) {
                case IN:
                    return evaluateInIterable(property, (Iterable) second, propertyDefinition);
                case NOT_IN:
                    return !evaluateInIterable(property, (Iterable) second, propertyDefinition);
                default:
                    throw new VertexiumException("Not implemented: " + this);
            }
        }

        if (second.getClass().isArray()) {
            switch (this) {
                case IN:
                    return evaluateInIterable(property, (Object[]) second, propertyDefinition);
                case NOT_IN:
                    return !evaluateInIterable(property, (Object[]) second, propertyDefinition);
                default:
                    throw new VertexiumException("Not implemented: " + this);
            }
        }

        throw new VertexiumException("Not implemented 'Contains' type. Expected Iterable found " + second.getClass().getName());
    }

    private boolean evaluateInIterable(Property property, Iterable second, PropertyDefinition propertyDefinition) {
        Object first = property.getValue();
        for (Object o : second) {
            if (Compare.evaluate(first, Compare.EQUAL, o, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateInIterable(Property property, Object[] second, PropertyDefinition propertyDefinition) {
        Object first = property.getValue();
        for (Object o : second) {
            if (Compare.evaluate(first, Compare.EQUAL, o, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }
}
