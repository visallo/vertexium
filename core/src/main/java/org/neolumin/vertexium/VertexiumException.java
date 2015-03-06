package org.neolumin.vertexium;

public class VertexiumException extends RuntimeException {
    public VertexiumException(Exception e) {
        super(e);
    }

    public VertexiumException(String msg, Exception e) {
        super(msg, e);
    }

    public VertexiumException(String msg) {
        super(msg);
    }
}
