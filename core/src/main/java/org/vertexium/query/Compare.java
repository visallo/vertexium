package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.type.GeoShape;

import java.util.Collection;
import java.util.Date;

public enum Compare implements Predicate {
    EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, STARTS_WITH;

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

        switch (comparePredicate) {
            case EQUAL:
                if (null == first) {
                    return second == null;
                }
                if (propertyDefinition != null && propertyDefinition.getTextIndexHints().size() > 0 && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                    return false;
                }
                return compare(first, second) == 0;
            case NOT_EQUAL:
                if (null == first) {
                    return second != null;
                }
                return compare(first, second) != 0;
            case GREATER_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return compare(first, second) >= 1;
            case LESS_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return compare(first, second) <= -1;
            case GREATER_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return compare(first, second) >= 0;
            case LESS_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return compare(first, second) <= 0;
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
            default:
                throw new IllegalArgumentException("Invalid compare: " + comparePredicate);
        }
    }

    private static int compare(Object first, Object second) {
        if (first instanceof StreamingPropertyValue && ((StreamingPropertyValue) first).getValueType() == String.class) {
            first = ((StreamingPropertyValue) first).readToString();
        }
        if (second instanceof StreamingPropertyValue && ((StreamingPropertyValue) second).getValueType() == String.class) {
            second = ((StreamingPropertyValue) second).readToString();
        }

        if (first instanceof String) {
            first = ((String) first).toLowerCase();
        }
        if (second instanceof String) {
            second = ((String) second).toLowerCase();
        }

        if (first instanceof Long && second instanceof Long) {
            long firstLong = (long) first;
            long secondLong = (long) second;
            return Long.compare(firstLong, secondLong);
        }
        if (first instanceof Integer && second instanceof Integer) {
            int firstInt = (int) first;
            int secondInt = (int) second;
            return Integer.compare(firstInt, secondInt);
        }
        if (first instanceof Number && second instanceof Number) {
            double firstDouble = ((Number) first).doubleValue();
            double secondDouble = ((Number) second).doubleValue();
            return Double.compare(firstDouble, secondDouble);
        }
        if (first instanceof Number && second instanceof String) {
            try {
                double firstDouble = ((Number) first).doubleValue();
                double secondDouble = Double.parseDouble(second.toString());
                return Double.compare(firstDouble, secondDouble);
            } catch (NumberFormatException ex) {
                return -1;
            }
        }
        if (first instanceof String && second instanceof Number) {
            try {
                double firstDouble = Double.parseDouble(first.toString());
                double secondDouble = ((Number) second).doubleValue();
                return Double.compare(firstDouble, secondDouble);
            } catch (NumberFormatException ex) {
                return 1;
            }
        }
        if (first instanceof Comparable) {
            return ((Comparable) first).compareTo(second);
        }
        if (second instanceof Comparable) {
            return ((Comparable) second).compareTo(first);
        }
        return first.equals(second) ? 0 : 1;
    }
}
