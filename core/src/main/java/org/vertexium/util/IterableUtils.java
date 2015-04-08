package org.vertexium.util;

import java.io.Closeable;
import java.lang.reflect.Array;
import java.util.*;

public class IterableUtils {
    @SuppressWarnings("unchecked")
    public static <T> List<T> toList(Iterable<? extends T> iterable) {
        if (iterable instanceof List) {
            return (List<T>) iterable;
        }
        List<T> results = new ArrayList<T>();
        for (T o : iterable) {
            results.add(o);
        }
        close(iterable);
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> toSet(Iterable<? extends T> iterable) {
        if (iterable instanceof Set) {
            return (Set<T>) iterable;
        }
        Set<T> results = new HashSet<T>();
        for (T o : iterable) {
            results.add(o);
        }
        close(iterable);
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> toSet(T[] iterable) {
        Set<T> results = new HashSet<T>();
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
        close(iterable);
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
            throw new IllegalStateException("No items found.");
        }

        T result = i.next();

        if (i.hasNext()) {
            close(it);
            throw new IllegalStateException("More than 1 item found.");
        }

        close(it);
        return result;
    }

    private static <T> void close(Iterable<? extends T> it) {
        if (it instanceof Closeable) {
            CloseableUtils.closeQuietly((Closeable) it);
        }
    }

    public static <T> T singleOrDefault(final Iterable<? extends T> it, T defaultValue) {
        Iterator<? extends T> i = it.iterator();
        if (!i.hasNext()) {
            return defaultValue;
        }

        T result = i.next();

        if (i.hasNext()) {
            close(it);
            throw new IllegalStateException("More than 1 item found.");
        }

        close(it);
        return result;
    }
}
