package org.vertexium.test.util;

import org.vertexium.util.CloseableUtils;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class IterableUtils {
    public static <T> void assertContains(Object expected, Iterable<T> iterable) {
        StringBuilder found = new StringBuilder();
        boolean first = true;
        Iterator<T> iterator = iterable.iterator();
        try {
            while (iterator.hasNext()) {
                T o = iterator.next();
                if (expected.equals(o)) {
                    return;
                }
                if (!first) {
                    found.append(", ");
                }
                found.append(o);
                first = false;
            }
        } finally {
            CloseableUtils.closeQuietly(iterator);
        }

        assertTrue(false, "Iterable does not contain [" + expected + "], found [" + found + "]");
    }
}
