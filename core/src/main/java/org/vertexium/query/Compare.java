package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.type.GeoShape;
import org.vertexium.util.ObjectUtils;

import java.util.Collection;
import java.util.Date;

public enum Compare implements Predicate {
    EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, STARTS_WITH, RANGE;

    @Override
    public boolean evaluate(final Iterable<Property> properties, final Object second, Collection<PropertyDefinition> propertyDefinitions) {
        boolean foundProperties = false;
        for (Property property : properties) {
            foundProperties = true;
            PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(propertyDefinitions, property.getName());
            if (evaluate(property, second, propertyDefinition)) {
                return true;
            }
        }

        if (!foundProperties && this.equals(NOT_EQUAL)) {
            return true;
        }

        return false;
    }

    @Override
    public boolean evaluate(Object first, Object second, PropertyDefinition propertyDefinition) {
        Compare comparePredicate = this;
        return evaluate(first, comparePredicate, second, propertyDefinition);
    }

    @Override
    public void validate(PropertyDefinition propertyDefinition) {
    }


    private boolean evaluate(Property property, Object second, PropertyDefinition propertyDefinition) {
        Object first = property.getValue();
        Compare comparePredicate = this;

        return evaluate(first, comparePredicate, second, propertyDefinition);
    }

    static boolean evaluate(Object first, Compare comparePredicate, Object second, PropertyDefinition propertyDefinition) {
        if (first instanceof DateOnly) {
            first = ((DateOnly) first).getDate();
            if (second instanceof Date) {
                second = new DateOnly((Date) second).getDate();
            }
        }
        if (second instanceof DateOnly) {
            second = ((DateOnly) second).getDate();
            if (first instanceof Date) {
                first = new DateOnly((Date) first).getDate();
            }
        }
        if (first instanceof ElementType) {
            first = ((ElementType) first).name();
        }
        if (second instanceof ElementType) {
            second = ((ElementType) second).name();
        }

        switch (comparePredicate) {
            case EQUAL:
                if (null == first) {
                    return second == null;
                }
                if (propertyDefinition != null && propertyDefinition.getTextIndexHints().size() > 0 && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                    return false;
                }
                return ObjectUtils.compare(first, second) == 0;
            case NOT_EQUAL:
                if (null == first) {
                    return second != null;
                }
                return ObjectUtils.compare(first, second) != 0;
            case GREATER_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) >= 1;
            case LESS_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) <= -1;
            case GREATER_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) >= 0;
            case LESS_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) <= 0;
            case STARTS_WITH:
                if (!(second instanceof String)) {
                    throw new VertexiumException("STARTS_WITH may only be used to query String values");
                }
                if (null == first) {
                    return second == null;
                }
                if (propertyDefinition != null && propertyDefinition.getTextIndexHints().size() > 0 && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                    return false;
                }
                if (first instanceof GeoShape) {
                    return ((GeoShape) first).getDescription().startsWith((String) second);
                }
                return first.toString().startsWith((String) second);
            case RANGE:
                if (first instanceof Range) {
                    return ((Range) first).isInRange(second);
                } else if (second instanceof Range) {
                    return ((Range) second).isInRange(first);
                } else {
                    throw new IllegalArgumentException("Invalid range values: " + first + ", " + second);
                }
            default:
                throw new IllegalArgumentException("Invalid compare: " + comparePredicate);
        }
    }
}
