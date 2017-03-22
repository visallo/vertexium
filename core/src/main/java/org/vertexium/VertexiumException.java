package org.vertexium;

public class VertexiumException extends RuntimeException {
    private static final long serialVersionUID = 6952596657973758922L;

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
