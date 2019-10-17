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
}
