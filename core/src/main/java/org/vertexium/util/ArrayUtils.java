package org.vertexium.util;

public class ArrayUtils {
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

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

    public static String toHex(byte[] array) {
        char[] hexChars = new char[array.length * 2];
        for (int j = 0; j < array.length; j++) {
            int v = array[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
