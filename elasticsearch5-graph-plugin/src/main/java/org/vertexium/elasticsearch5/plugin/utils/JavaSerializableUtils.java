package org.vertexium.elasticsearch5.plugin.utils;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHitField;
import org.vertexium.elasticsearch5.plugin.VertexiumElasticsearchPluginException;

import java.io.*;
import java.util.Base64;
import java.util.function.Supplier;

public class JavaSerializableUtils {
    public static byte[] objectToBytes(Object obj) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new VertexiumElasticsearchPluginException("Could not convert object to bytes", e);
        }
    }

    public static <T> T bytesToObject(byte[] bytes) {
        try {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return (T) ois.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new VertexiumElasticsearchPluginException("Could not conver bytes to object", e);
        }
    }

    public static <T> T copy(T value) {
        return (T) bytesToObject(objectToBytes(value));
    }

    public static <T> T fromField(Object fieldValue, Supplier<T> defaultSupplier) {
        if (fieldValue == null) {
            return defaultSupplier.get();
        }
        byte[] bytes;
        if (fieldValue instanceof byte[]) {
            bytes = (byte[]) fieldValue;
        } else if (fieldValue instanceof String) {
            bytes = Base64.getDecoder().decode((String) fieldValue);
        } else if (fieldValue instanceof BytesArray) {
            bytes = ((BytesArray) fieldValue).array();
        } else if (fieldValue instanceof SearchHitField) {
            SearchHitField searchHitField = (SearchHitField) fieldValue;
            if (searchHitField.getValues().size() == 1) {
                return fromField(searchHitField.getValues().get(0), defaultSupplier);
            } else if (searchHitField.getValues().size() == 0) {
                return fromField(null, defaultSupplier);
            } else {
                throw new VertexiumElasticsearchPluginException(String.format(
                    "invalid field value: %s (expected 1 value found %d)",
                    fieldValue.getClass().getName(),
                    searchHitField.getValues().size()
                ));
            }
        } else {
            throw new VertexiumElasticsearchPluginException("invalid field value: " + fieldValue.getClass().getName());
        }
        return bytesToObject(bytes);
    }
}
