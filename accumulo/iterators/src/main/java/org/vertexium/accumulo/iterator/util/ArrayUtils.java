package org.vertexium.accumulo.iterator.util;

public class ArrayUtils {
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

    public static boolean equals(byte[] a, byte[] b, int bOffset, int bLength) {
        return equals(a, 0, a.length, b, bOffset, bLength);
    }

    public static boolean equals(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength) {
        if (aLength != bLength) {
            return false;
        }
        for (int i = 0; i < aLength; i++) {
            if (a[i + aOffset] != b[i + bOffset]) {
                return false;
            }
        }
        return true;
    }

    public static int computeHash(byte[] data, int offset, int length) {
        int hash = 1;
        for (int i = offset; i < offset + length; i++) {
            hash = (31 * hash) + data[i];
        }
        return hash;
    }
}
