package org.vertexium;

public class VertexiumPropertyNotDefinedException extends VertexiumException {
    private static final long serialVersionUID = -2506524613627669153L;

    public VertexiumPropertyNotDefinedException(Exception e) {
        super(e);
    }

    public VertexiumPropertyNotDefinedException(String msg, Throwable e) {
        super(msg, e);
    }

    public VertexiumPropertyNotDefinedException(String msg) {
        super(msg);
    }
}
