package org.vertexium.accumulo.iterator.model;

public class VertexiumAccumuloIteratorException extends RuntimeException {
    public VertexiumAccumuloIteratorException(String message) {
        super(message);
    }

    public VertexiumAccumuloIteratorException(String message, Throwable cause) {
        super(message, cause);
    }
}
