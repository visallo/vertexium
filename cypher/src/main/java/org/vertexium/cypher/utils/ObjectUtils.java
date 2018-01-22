package org.vertexium.cypher.utils;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ObjectUtils {
    public static boolean equals(Object leftObj, Object rightObj) {
        return compare(leftObj, rightObj) == 0;
    }

    public static int compare(Object leftObj, Object rightObj) {
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

        if (leftObj instanceof Collection && rightObj instanceof Collection) {
            return compareCollections((Collection) leftObj, (Collection) rightObj);
        }

        if (!(leftObj instanceof Comparable)) {
            throw new ClassCastException(leftObj.getClass().getName() + " does not implement " + Comparable.class.getName());
        }
        Comparable left = (Comparable) leftObj;
        try {
            if (leftObj instanceof Number && rightObj instanceof Number) {
                Number leftNumber = (Number) leftObj;
                Number rightNumber = (Number) rightObj;
                if (leftObj instanceof Double || leftObj instanceof Float || rightObj instanceof Double || rightObj instanceof Float) {
                    return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
                }
                return Long.compare(leftNumber.longValue(), rightNumber.longValue());
            }

            if (left instanceof Number && !(rightObj instanceof Number)) {
                return -1;
            }

            if (rightObj instanceof Number && !(left instanceof Number)) {
                return 1;
            }

            return left.compareTo(rightObj);
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

    public static Number addNumbers(Number left, Number right) {
        Number leftNumber = left;
        Number rightNumber = right;
        if (leftNumber instanceof Double || leftNumber instanceof Float
                || rightNumber instanceof Double || rightNumber instanceof Float) {
            return leftNumber.doubleValue() + rightNumber.doubleValue();
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return leftNumber.longValue() + rightNumber.longValue();
        }
        return leftNumber.intValue() + rightNumber.intValue();
    }

    public static Number sumNumbers(Stream<?> stream) {
        return stream
                .filter(Objects::nonNull)
                .map(o -> {
                    VertexiumCypherTypeErrorException.assertType(o, Number.class);
                    return (Number) o;
                })
                .reduce(0L, ObjectUtils::addNumbers);
    }

    public static double averageNumbers(Stream<?> stream) {
        AtomicInteger count = new AtomicInteger();
        return stream
                .filter(Objects::nonNull)
                .map(o -> {
                    VertexiumCypherTypeErrorException.assertType(o, Number.class);
                    count.incrementAndGet();
                    return (Number) o;
                })
                .reduce(0L, ObjectUtils::addNumbers)
                .doubleValue() / count.doubleValue();
    }
}
