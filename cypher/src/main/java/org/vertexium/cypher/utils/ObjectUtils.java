package org.vertexium.cypher.utils;

import org.vertexium.Edge;
import org.vertexium.Vertex;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
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

            try {
                return left.compareTo(rightObj);
            } catch (Exception ex) {
                throw new VertexiumCypherException("Could not compare: " + left + " ?= " + rightObj, ex);
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

    public static Number addNumbers(Number leftNumber, Number rightNumber) {
        if (leftNumber instanceof Double || leftNumber instanceof Float
            || rightNumber instanceof Double || rightNumber instanceof Float) {
            return leftNumber.doubleValue() + rightNumber.doubleValue();
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return leftNumber.longValue() + rightNumber.longValue();
        }
        return leftNumber.intValue() + rightNumber.intValue();
    }

    public static Number subtractNumbers(Number leftNumber, Number rightNumber) {
        if (leftNumber instanceof Double || leftNumber instanceof Float
            || rightNumber instanceof Double || rightNumber instanceof Float) {
            return leftNumber.doubleValue() - rightNumber.doubleValue();
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return leftNumber.longValue() - rightNumber.longValue();
        }
        return leftNumber.intValue() - rightNumber.intValue();
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

    public static Object divideNumbers(Number numerator, Number denominator) {
        if (numerator instanceof Double || denominator instanceof Double) {
            return numerator.doubleValue() / denominator.doubleValue();
        } else if (numerator instanceof Float || denominator instanceof Float) {
            return numerator.floatValue() / denominator.floatValue();
        } else if (numerator instanceof Long || denominator instanceof Long) {
            return numerator.longValue() / denominator.longValue();
        } else if (numerator instanceof Integer || denominator instanceof Integer) {
            return numerator.intValue() / denominator.intValue();
        }
        throw new VertexiumCypherNotImplemented("cannot divide numbers");
    }

    public static Object modNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double) {
            return a.doubleValue() % b.doubleValue();
        } else if (a instanceof Float || b instanceof Float) {
            return a.floatValue() % b.floatValue();
        } else if (a instanceof Long || b instanceof Long) {
            return a.longValue() % b.longValue();
        } else if (a instanceof Integer || b instanceof Integer) {
            return a.intValue() % b.intValue();
        }
        throw new VertexiumCypherNotImplemented("cannot mod numbers");
    }

    public static Object powerNumbers(Number a, Number b) {
        return Math.pow(a.doubleValue(), b.doubleValue());
    }

    public static Object multiplyNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double) {
            return a.doubleValue() * b.doubleValue();
        } else if (a instanceof Float || b instanceof Float) {
            return a.floatValue() * b.floatValue();
        } else if (a instanceof Long || b instanceof Long) {
            return a.longValue() * b.longValue();
        } else if (a instanceof Integer || b instanceof Integer) {
            return a.intValue() * b.intValue();
        }
        throw new VertexiumCypherNotImplemented("cannot multiply numbers");
    }

    public static List<Object> arrayToList(Object arr) {
        int length = Array.getLength(arr);
        ArrayList<Object> result = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            result.add(Array.get(arr, i));
        }
        return result;
    }

    /**
     * Similar to {@link org.vertexium.util.StreamUtils#distinctBy(Function)} but does a deep compare
     */
    public static <T> Predicate<T> distinctByDeep(Function<? super T, ?> fn) {
        Set<String> seen = ConcurrentHashMap.newKeySet();
        return t -> {
            return seen.add(toKey(fn.apply(t)));
        };
    }

    private static String toKey(Object o) {
        if (o == null) {
            return "--null--";
        }

        if (o instanceof String) {
            return (String) o;
        }

        if (o instanceof Vertex) {
            return "v" + ((Vertex) o).getId();
        }

        if (o instanceof Edge) {
            return "e" + ((Edge) o).getId();
        }

        if (o instanceof Map<?, ?>) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) o).entrySet()) {
                sb
                    .append(toKey(entry.getKey()))
                    .append(toKey(entry.getValue()));
            }
            return sb.toString();
        }

        if (o instanceof Number) {
            double d = ((Number) o).doubleValue();
            return Double.toString(d);
        }

        if (o.getClass().isArray()) {
            StringBuilder sb = new StringBuilder();
            int len = Array.getLength(o);
            for (int i = 0; i < len; i++) {
                sb.append(toKey(Array.get(o, i)));
            }
            return sb.toString();
        }

        throw new VertexiumCypherNotImplemented("toKey: " + o.getClass().getName());
    }
}
