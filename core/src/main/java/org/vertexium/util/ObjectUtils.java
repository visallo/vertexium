package org.vertexium.util;

import org.vertexium.VertexiumException;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.type.GeoShape;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ObjectUtils {
    public static boolean equals(Object leftObj, Object rightObj) {
        return compare(leftObj, rightObj) == 0;
    }

    public static int compare(Object leftObj, Object rightObj) {
        try {
            if (leftObj == null && rightObj == null) {
                return 0;
            }
            if (leftObj == null) {
                return 1;
            }
            if (rightObj == null) {
                return -1;
            }

            if (leftObj instanceof Stream && rightObj instanceof Stream) {
                return compareStreams((Stream) leftObj, (Stream) rightObj);
            }

            if (leftObj instanceof Stream) {
                leftObj = ((Stream) leftObj).collect(Collectors.toList());
            }

            if (rightObj instanceof Stream) {
                rightObj = ((Stream) rightObj).collect(Collectors.toList());
            }

            if (leftObj instanceof StreamingPropertyValue && ((StreamingPropertyValue) leftObj).getValueType() == String.class) {
                leftObj = ((StreamingPropertyValue) leftObj).readToString();
            }
            if (rightObj instanceof StreamingPropertyValue && ((StreamingPropertyValue) rightObj).getValueType() == String.class) {
                rightObj = ((StreamingPropertyValue) rightObj).readToString();
            }

            if (leftObj instanceof String) {
                leftObj = ((String) leftObj).toLowerCase();
            }
            if (rightObj instanceof String) {
                rightObj = ((String) rightObj).toLowerCase();
            }

            if (leftObj instanceof Long && rightObj instanceof Long) {
                long firstLong = (long) leftObj;
                long secondLong = (long) rightObj;
                return Long.compare(firstLong, secondLong);
            }

            if (leftObj instanceof Integer && rightObj instanceof Integer) {
                int firstInt = (int) leftObj;
                int secondInt = (int) rightObj;
                return Integer.compare(firstInt, secondInt);
            }

            if (leftObj instanceof Number && rightObj instanceof Number) {
                Number leftNumber = (Number) leftObj;
                Number rightNumber = (Number) rightObj;
                if (leftObj instanceof Double || leftObj instanceof Float || rightObj instanceof Double || rightObj instanceof Float) {
                    return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
                }
                return Long.compare(leftNumber.longValue(), rightNumber.longValue());
            }

            if (leftObj instanceof Number && rightObj instanceof String) {
                try {
                    double firstDouble = ((Number) leftObj).doubleValue();
                    double secondDouble = Double.parseDouble(rightObj.toString());
                    return Double.compare(firstDouble, secondDouble);
                } catch (NumberFormatException ex) {
                    return -1;
                }
            }

            if (leftObj instanceof String && rightObj instanceof Number) {
                try {
                    double firstDouble = Double.parseDouble(leftObj.toString());
                    double secondDouble = ((Number) rightObj).doubleValue();
                    return Double.compare(firstDouble, secondDouble);
                } catch (NumberFormatException ex) {
                    return 1;
                }
            }

            if (leftObj instanceof GeoShape && rightObj instanceof String) {
                String description = ((GeoShape) leftObj).getDescription();
                return description == null ? -1 : description.toLowerCase().compareTo((String) rightObj);
            }

            if (leftObj instanceof Collection && rightObj instanceof Collection) {
                return compareCollections((Collection) leftObj, (Collection) rightObj);
            }

            if (leftObj instanceof Number && !(rightObj instanceof Number)) {
                return -1;
            }

            if (rightObj instanceof Number && !(leftObj instanceof Number)) {
                return 1;
            }

            try {
                if (leftObj instanceof Comparable) {
                    return ((Comparable) leftObj).compareTo(rightObj);
                }
                if (rightObj instanceof Comparable) {
                    return ((Comparable) rightObj).compareTo(leftObj);
                }
            } catch (Exception ex) {
                throw new VertexiumException("Could not compare: " + leftObj + " ?= " + rightObj, ex);
            }
        } catch (Exception ex) {
            if (ex instanceof ClassCastException) {
                throw ex;
            }
            throw new VertexiumException(
                String.format(
                    "Could not compare \"%s\" (%s) to \"%s\" (%s)",
                    leftObj,
                    leftObj == null ? "null" : leftObj.getClass().getName(),
                    rightObj,
                    rightObj == null ? "null" : rightObj.getClass().getName()
                ),
                ex
            );
        }
        return leftObj.equals(rightObj) ? 0 : 1;
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
