package org.vertexium.elasticsearch2.utils;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.get.GetField;

public class GetResponseUtil {
    public static String getFieldValueString(GetResponse getResponse, String fieldName) {
        GetField field = getResponse.getField(fieldName);
        if (field == null) {
            return null;
        }
        return (String) field.getValue();
    }

    public static Long getFieldValueLong(GetResponse getResponse, String fieldName) {
        GetField field = getResponse.getField(fieldName);
        if (field == null) {
            return null;
        }
        Object value = field.getValue();
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        return (Long) value;
    }
}
