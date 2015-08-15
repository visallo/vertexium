package org.vertexium.util;

import java.lang.reflect.Array;
import java.util.*;

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
        List<? extends T> list = toList(iterable);
        T[] array = (T[]) Array.newInstance(type, list.size());
        return list.toArray(array);
    }

    public static <T> int count(Iterable<T> iterable) {
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

    public static <T> Iterable<T> toIterable(final T[] arr) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
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
}
