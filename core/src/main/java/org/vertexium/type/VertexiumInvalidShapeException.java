package org.vertexium.type;

import org.vertexium.VertexiumException;

public class VertexiumInvalidShapeException extends VertexiumException {
    public VertexiumInvalidShapeException(Exception e) {
        super(e);
    }

    public VertexiumInvalidShapeException(String msg, Throwable e) {
        super(msg, e);
    }

    public VertexiumInvalidShapeException(String msg) {
        super(msg);
    }
}
