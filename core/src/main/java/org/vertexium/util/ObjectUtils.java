package org.vertexium.util;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.type.GeoShape;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public class ObjectUtils {
    public static boolean equals(Object leftObj, Object rightObj) {
        return compare(leftObj, rightObj) == 0;
    }

    @SuppressWarnings("unchecked")
    public static int compare(Object first, Object second) {
        if (first == null && second == null) {
            return 0;
        }

        if (first == null) {
            return 1;
        }
        if (second == null) {
            return -1;
        }

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
        if (first instanceof GeoShape && second instanceof String) {
            String description = ((GeoShape) first).getDescription();
            return description == null ? -1 : description.toLowerCase().compareTo((String) second);
        }
        if (first instanceof Comparable) {
            return ((Comparable) first).compareTo(second);
        }
        if (second instanceof Comparable) {
            return ((Comparable) second).compareTo(first);
        }
        return first.equals(second) ? 0 : 1;
    }

    private static int compareCollections(Collection leftObj, Collection rightObj) {
        int sizeCompare = Integer.compare(leftObj.size(), rightObj.size());
        if (sizeCompare != 0) {
            return sizeCompare;
        }

        Iterator leftIt = leftObj.iterator();
        Iterator rightIt = rightObj.iterator();
        while (leftIt.hasNext() && rightIt.hasNext()) {
            int c = compare(leftIt.next(), rightIt.next());
            if (c != 0) {
                return c;
            }
        }

        return 0;
    }

    private static int compareStreams(Stream leftObj, Stream rightObj) {
        Iterator leftIt = leftObj.iterator();
        Iterator rightIt = rightObj.iterator();
        while (leftIt.hasNext() && rightIt.hasNext()) {
            int c = compare(leftIt.next(), rightIt.next());
            if (c != 0) {
                return c;
            }
        }

        if (leftIt.hasNext()) {
            return -1;
        }
        if (rightIt.hasNext()) {
            return 1;
        }
        return 0;
    }
}
