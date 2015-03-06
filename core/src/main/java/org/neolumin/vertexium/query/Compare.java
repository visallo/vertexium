package org.neolumin.vertexium.query;

import org.neolumin.vertexium.DateOnly;
import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.PropertyDefinition;
import org.neolumin.vertexium.TextIndexHint;
import org.neolumin.vertexium.property.StreamingPropertyValue;

import java.util.Date;
import java.util.Map;

public enum Compare implements Predicate {
    EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, IN;

    @Override
    public boolean evaluate(final Iterable<Property> properties, final Object second, Map<String, PropertyDefinition> propertyDefinitions) {
        for (Property property : properties) {
            PropertyDefinition propertyDefinition = propertyDefinitions.get(property.getName());
            if (evaluate(property, second, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Property property, Object second, PropertyDefinition propertyDefinition) {
        Object first = property.getValue();

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

        switch (this) {
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
            case IN:
                return evaluateIn(first, (Object[]) second);
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }

    private int compare(Object first, Object second) {
        if (first instanceof StreamingPropertyValue && ((StreamingPropertyValue) first).getValueType() == String.class) {
            first = ((StreamingPropertyValue) first).readToString();
        }
        if (second instanceof StreamingPropertyValue && ((StreamingPropertyValue) second).getValueType() == String.class) {
            second = ((StreamingPropertyValue) second).readToString();
        }

        if (first instanceof Number && second instanceof Number) {
            double firstDouble = ((Number) first).doubleValue();
            double secondDouble = ((Number) second).doubleValue();
            return Double.compare(firstDouble, secondDouble);
        }
        if (first instanceof Number && second instanceof String) {
            double firstDouble = ((Number) first).doubleValue();
            double secondDouble = Double.parseDouble(second.toString());
            return Double.compare(firstDouble, secondDouble);
        }
        if (first instanceof String && second instanceof Number) {
            double firstDouble = Double.parseDouble(first.toString());
            double secondDouble = ((Number) second).doubleValue();
            return Double.compare(firstDouble, secondDouble);
        }
        if (first instanceof Comparable) {
            return ((Comparable) first).compareTo(second);
        }
        if (second instanceof Comparable) {
            return ((Comparable) second).compareTo(first);
        }
        return first.equals(second) ? 0 : 1;
    }

    private boolean evaluateIn(Object first, Object[] second) {
        for (Object o : second) {
            if (first.equals(o)) {
                return true;
            }
        }
        return false;
    }
}
