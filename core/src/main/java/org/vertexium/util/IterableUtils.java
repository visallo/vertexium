package org.vertexium.util;

import org.vertexium.Element;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.CloseableUtils.closeQuietly;

public class IterableUtils {
    @SuppressWarnings("unchecked")
    public static <T> List<T> toList(Iterable<? extends T> iterable) {
        if (iterable instanceof List) {
            return (List<T>) iterable;
        }
        List<T> results = new ArrayList<>();
        for (T o : iterable) {
            results.add(o);
        }
        closeQuietly(iterable);
        return results;
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> results = new ArrayList<>();
        while (iterator.hasNext()) {
            T o = iterator.next();
            results.add(o);
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> toSet(Iterable<? extends T> iterable) {
        if (iterable instanceof Set) {
            return (Set<T>) iterable;
        }
        Set<T> results = new HashSet<>();
        for (T o : iterable) {
            results.add(o);
        }
        closeQuietly(iterable);
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> toSet(T[] iterable) {
        Set<T> results = new HashSet<>();
        Collections.addAll(results, iterable);
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] toArray(Iterable<? extends T> iterable, Class<T> type) {
        if (iterable instanceof Collection) {
            T[] array = (T[]) Array.newInstance(type, ((Collection) iterable).size());
            return ((Collection<T>) iterable).toArray(array);
        }
        List<? extends T> list = toList(iterable);
        T[] array = (T[]) Array.newInstance(type, list.size());
        return list.toArray(array);
    }

    public static <T> int count(Iterable<T> iterable) {
        if (iterable instanceof Collection) {
            return ((Collection) iterable).size();
        }

        int count = 0;
        for (T ignore : iterable) {
            count++;
        }
        closeQuietly(iterable);
        return count;
    }

    public static <T> int count(Iterator<T> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        closeQuietly(iterator);
        return count;
    }

    public static <T> boolean isEmpty(Iterable<T> iterable) {
        Iterator<T> iterator = iterable.iterator();
        try {
            return !iterator.hasNext();
        } finally {
            closeQuietly(iterator);
        }
    }

    public static <T> Iterable<T> toIterable(final T[] arr) {
        return () -> new Iterator<T>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < arr.length;
            }

            @Override
            public T next() {
                return arr[index++];
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not supported");
            }
        };
    }

    public static <T> T single(final Iterable<? extends T> it) {
        Iterator<? extends T> i = it.iterator();
        if (!i.hasNext()) {
            closeQuietly(i, it);
            throw new IllegalStateException("No items found.");
        }

        T result = i.next();

        if (i.hasNext()) {
            closeQuietly(i, it);
            throw new IllegalStateException("More than 1 item found.");
        }

        closeQuietly(i, it);
        return result;
    }

    public static <T> T singleOrDefault(Stream<? extends T> stream, T defaultValue) {
        List<? extends T> items = stream.limit(2).collect(Collectors.toList());
        if (items.size() > 1) {
            throw new IllegalStateException("More than 1 item found.");
        }
        return items.size() == 1 ? items.get(0) : defaultValue;
    }

    public static <T> T singleOrDefault(final Iterable<? extends T> it, T defaultValue) {
        Iterator<? extends T> i = it.iterator();
        if (!i.hasNext()) {
            closeQuietly(i, it);
            return defaultValue;
        }

        T result = i.next();

        if (i.hasNext()) {
            T nextValue = i.next();
            closeQuietly(i, it);
            throw new IllegalStateException("More than 1 item found. [" + result + ", " + nextValue + "...]");
        }

        closeQuietly(i, it);
        return result;
    }

    public static String join(Iterable items, String sep) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (Object o : items) {
            if (!first) {
                sb.append(sep);
            }
            sb.append(o);
            first = false;
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public static Iterable<Element> toElementIterable(Iterable<? extends Element> elements) {
        return (Iterable<Element>) elements;
    }

    public static <T extends Element> Map<String, T> toMapById(Iterable<T> elements) {
        Map<String, T> result = new HashMap<>();
        for (T element : elements) {
            if (element != null) {
                result.put(element.getId(), element);
            }
        }
        return result;
    }
}
