package org.vertexium.util;

public class ArrayUtils {
    /**
     * Determines if all values in a1 appear in a2 and that all values in a2 appear in a2.
     */
    public static <T> boolean intersectsAll(T[] a1, T[] a2) {
        for (T anA1 : a1) {
            if (!contains(a2, anA1)) {
                return false;
            }
        }

        for (T anA2 : a2) {
            if (!contains(a1, anA2)) {
                return false;
            }
        }

        return true;
    }

    public static <T> boolean contains(T[] a1, T v) {
        for (T anA1 : a1) {
            if (anA1.equals(v)) {
                return true;
            }
        }
        return false;
    }

    public static boolean startsWith(byte[] array, byte[] seq) {
        if (array.length < seq.length) {
            return false;
        }
        for (int i = 0; i < seq.length; i++) {
            if (array[i] != seq[i]) {
                return false;
            }
        }
        return true;
    }
}
