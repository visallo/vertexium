package org.vertexium.accumulo.iterator.model;

import java.util.ArrayList;
import java.util.List;

public abstract class KeyBase {
    public static final char VALUE_SEPARATOR = '\u001f';

    public static String[] splitOnValueSeparator(String s) {
        List<String> results = new ArrayList<>();
        int last = 0;
        int i = s.indexOf(VALUE_SEPARATOR);
        while (true) {
            if (i > 0) {
                results.add(s.substring(last, i));
                last = i + 1;
                i = s.indexOf(VALUE_SEPARATOR, last);
            } else {
                results.add(s.substring(last));
                break;
            }
        }
        return results.toArray(new String[results.size()]);
    }

    public static String[] splitOnValueSeparator(String s, int partCount) {
        String[] results = new String[partCount];
        int last = 0;
        int i = s.indexOf(VALUE_SEPARATOR);
        int partIndex = 0;
        while (true) {
            if (i > 0) {
                results[partIndex++] = s.substring(last, i);
                if (partIndex >= partCount) {
                    throw new VertexiumAccumuloIteratorException("Invalid number of parts for '" + s + "'. Expected " + partCount + " found " + partIndex);
                }
                last = i + 1;
                i = s.indexOf(VALUE_SEPARATOR, last);
            } else {
                results[partIndex++] = s.substring(last);
                break;
            }
        }
        if (partIndex != partCount) {
            throw new VertexiumAccumuloIteratorException("Invalid number of parts for '" + s + "'. Expected " + partCount + " found " + partIndex);
        }
        return results;
    }

    public static void assertNoValueSeparator(String str) {
        if (str.indexOf(VALUE_SEPARATOR) >= 0) {
            throw new VertexiumInvalidKeyException("String cannot contain '" + VALUE_SEPARATOR + "' (0x1f): " + str);
        }
    }
}
