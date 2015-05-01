package org.vertexium;

public class VertexiumException extends RuntimeException {
    public VertexiumException(Exception e) {
        super(e);
    }

    public VertexiumException(String msg, Throwable e) {
        super(msg, e);
    }

    public VertexiumException(String msg) {
        super(msg);
    }
}
