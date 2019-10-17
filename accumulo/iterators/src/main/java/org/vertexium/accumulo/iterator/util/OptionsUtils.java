package org.vertexium.accumulo.iterator.util;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.model.SortDirection;
import org.vertexium.accumulo.iterator.model.VertexiumAccumuloIteratorException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class OptionsUtils {
    private static final String RECORD_SEPARATOR = "\u001f";
    private static final Pattern RECORD_SEPARATOR_PATTERN = Pattern.compile(Pattern.quote(RECORD_SEPARATOR));

    public static Set<ByteSequence> parseTextSet(String str) {
        if (str == null) {
            return null;
        }
        String[] parts = RECORD_SEPARATOR_PATTERN.split(str);
        Set<ByteSequence> results = new HashSet<>();
        for (String part : parts) {
            results.add(new ArrayByteSequence(part));
        }
        return results;
    }

    public static Set<String> parseSet(String str) {
        if (str == null) {
            return null;
        }
        String[] parts = RECORD_SEPARATOR_PATTERN.split(str);
        Set<String> results = new HashSet<>();
        Collections.addAll(results, parts);
        return results;
    }

    public static String textSetToString(Set<ByteSequence> set) {
        if (set == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (ByteSequence s : set) {
            if (!first) {
                sb.append(RECORD_SEPARATOR);
            }
            sb.append(ByteSequenceUtils.toString(s));
            first = false;
        }
        return sb.toString();
    }

    public static void addOption(IteratorSetting iteratorSettings, String key, String value) {
        if (value == null) {
            return;
        }
        iteratorSettings.addOption(key, value);
    }

    public static String setToString(Set<String> set) {
        if (set == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : set) {
            if (!first) {
                sb.append(RECORD_SEPARATOR);
            }
            sb.append(s);
            first = false;
        }
        return sb.toString();
    }

    public static Long parseLongOptional(String str) {
        if (str == null || str.length() == 0) {
            return null;
        }
        return Long.parseLong(str);
    }

    public static ElementType parseElementTypeRequired(String str) {
        if (str == null) {
            throw new VertexiumAccumuloIteratorException("Invalid element type: null");
        }
        try {
            return ElementType.valueOf(str.toUpperCase());
        } catch (Exception ex) {
            throw new VertexiumAccumuloIteratorException("Invalid ElementType: " + str, ex);
        }
    }

    public static SortDirection parseSortDirectionOptional(String str) {
        if (str == null || str.length() == 0) {
            return null;
        }
        try {
            return SortDirection.valueOf(str.toUpperCase());
        } catch (Exception ex) {
            throw new VertexiumAccumuloIteratorException("Invalid SortDirection: " + str, ex);
        }
    }
}
