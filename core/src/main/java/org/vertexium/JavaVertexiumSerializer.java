package org.vertexium;

import org.vertexium.util.JavaSerializableUtils;

public class JavaVertexiumSerializer implements VertexiumSerializer {
    private static final byte[] EMPTY = new byte[0];

    @Override
    public byte[] objectToBytes(Object object) {
        if (object == null) {
            return EMPTY;
        }
        return JavaSerializableUtils.objectToBytes(object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T bytesToObject(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return (T) JavaSerializableUtils.bytesToObject(bytes);
    }
}
