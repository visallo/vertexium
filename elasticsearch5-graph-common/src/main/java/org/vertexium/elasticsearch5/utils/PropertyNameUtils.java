package org.vertexium.elasticsearch5.utils;

import org.vertexium.elasticsearch5.models.Property;

public class PropertyNameUtils {
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
    public static final String FIELDNAME_DOT_REPLACEMENT = "-_-";

    public static String getPropertyNameWithVisibility(Property property) {
        return getPropertyNameWithVisibility(property.getName(), property.getVisibility());
    }

    public static String getPropertyNameWithVisibility(String propertyName, String propertyVisibility) {
        String visibilityHash = getVisibilityHash(propertyVisibility);
        return replaceFieldnameDots(propertyName) + "_" + visibilityHash;
    }

    public static String getVisibilityHash(String visibility) {
        return byteArrayToString(Murmur3.hash128(visibility.getBytes()));
    }

    private static String byteArrayToString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static String replaceFieldnameDots(String fieldName) {
        return fieldName.replace(".", FIELDNAME_DOT_REPLACEMENT);
    }
}
