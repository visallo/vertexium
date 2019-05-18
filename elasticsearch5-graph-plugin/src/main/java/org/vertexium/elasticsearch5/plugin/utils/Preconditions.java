package org.vertexium.elasticsearch5.plugin.utils;

public class Preconditions {
    public static void checkNotNull(Object o, String message, Object... messageArgs) {
        if (o == null) {
            throw new NullPointerException(String.format(message, messageArgs));
        }
    }
}
