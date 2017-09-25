package org.vertexium.query;

import org.vertexium.Property;
import org.vertexium.PropertyDefinition;
import org.vertexium.VertexiumException;
import org.vertexium.util.IterableUtils;

import java.util.Collection;

public enum Contains implements Predicate {
    IN, NOT_IN;

    @Override
    public boolean evaluate(Iterable<Property> properties, Object second, Collection<PropertyDefinition> propertyDefinitions) {
        if (IterableUtils.count(properties) == 0 && this == NOT_IN) {
            return true;
        }
        for (Property property : properties) {
            PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(propertyDefinitions, property.getName());
            if (evaluate(property.getValue(), second, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean evaluate(Object first, Object second, PropertyDefinition propertyDefinition) {
        if (second instanceof Iterable) {
            switch (this) {
                case IN:
                    return evaluateInIterable(first, (Iterable) second, propertyDefinition);
                case NOT_IN:
                    return !evaluateInIterable(first, (Iterable) second, propertyDefinition);
                default:
                    throw new VertexiumException("Not implemented: " + this);
            }
        }

        if (second.getClass().isArray()) {
            switch (this) {
                case IN:
                    return evaluateInIterable(first, (Object[]) second, propertyDefinition);
                case NOT_IN:
                    return !evaluateInIterable(first, (Object[]) second, propertyDefinition);
                default:
                    throw new VertexiumException("Not implemented: " + this);
            }
        }

        throw new VertexiumException("Not implemented 'Contains' type. Expected Iterable found " + second.getClass().getName());
    }

    @Override
    public boolean isSupported(PropertyDefinition propertyDefinition) {
        return true;
    }


    private boolean evaluateInIterable(Object first, Iterable second, PropertyDefinition propertyDefinition) {
        for (Object o : second) {
            if (Compare.evaluate(first, Compare.EQUAL, o, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateInIterable(Object first, Object[] second, PropertyDefinition propertyDefinition) {
        for (Object o : second) {
            if (Compare.evaluate(first, Compare.EQUAL, o, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }
}
