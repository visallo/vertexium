package org.vertexium.accumulo.iterator.util;

import java.util.HashSet;
import java.util.Set;

public class SetOfStringsEncoder {
    public static String encodeToString(Set<String> set) {
        StringBuilder result = new StringBuilder();
        for (String s : set) {
            result.append(encodeLength(s.length()));
            result.append(s);
        }
        return result.toString();
    }

    private static String encodeLength(int length) {
        return String.format("%08X", length);
    }

    public static Set<String> decodeFromString(String str) {
        Set<String> results = new HashSet<>();
        for (int i = 0; i < str.length(); ) {
            String lengthStr = str.substring(i, i + 8);
            int length = Integer.parseInt(lengthStr, 16);
            i += 8;
            String item = str.substring(i, i + length);
            i += length;
            results.add(item);
        }
        return results;
    }
}
