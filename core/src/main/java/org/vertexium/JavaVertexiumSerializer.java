package org.vertexium;

import org.vertexium.util.JavaSerializableUtils;

public class JavaVertexiumSerializer implements VertexiumSerializer {
    @Override
    public byte[] objectToBytes(Object object) {
        return JavaSerializableUtils.objectToBytes(object);
    }

    @Override
    public <T> T bytesToObject(byte[] bytes) {
        return (T) JavaSerializableUtils.bytesToObject(bytes);
    }
}
