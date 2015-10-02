package org.vertexium;

public interface VertexiumSerializer {
    byte[] objectToBytes(Object object);

    <T> T bytesToObject(byte[] bytes);
}
