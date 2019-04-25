package org.vertexium.util;

import org.vertexium.VertexiumException;

public class KeyUtils {
    public static final char VALUE_SEPARATOR = '\u001f';

    public static void checkKey(String key, String message) {
        if (key != null && key.indexOf(VALUE_SEPARATOR) >= 0) {
            throw new VertexiumException(String.format("%s. Cannot contain 0x%04x. key: %s", message, (byte) VALUE_SEPARATOR, key));
        }
    }
}
