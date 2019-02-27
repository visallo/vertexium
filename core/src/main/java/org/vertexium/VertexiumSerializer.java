package org.vertexium;

public interface VertexiumSerializer {
    byte[] objectToBytes(Object object);

    <T> T bytesToObject(byte[] bytes);

    default <T> T bytesToObject(Element sourceElement, byte[] bytes) {
        try {
            return bytesToObject(bytes);
        } catch (Exception ex) {
            throw new VertexiumException("Could not deserialize: " + sourceElement, ex);
        }
    }

    default <T> T bytesToObject(ExtendedDataRowId rowId, byte[] bytes) {
        try {
            return bytesToObject(bytes);
        } catch (Exception ex) {
            throw new VertexiumException("Could not deserialize: " + rowId, ex);
        }
    }
}
