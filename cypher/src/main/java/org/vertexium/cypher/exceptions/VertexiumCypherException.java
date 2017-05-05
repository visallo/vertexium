package org.vertexium.cypher.exceptions;

import org.vertexium.VertexiumException;

public class VertexiumCypherException extends VertexiumException {
    private static final long serialVersionUID = -2509514613627669153L;

    public VertexiumCypherException(Exception e) {
        super(e);
    }

    public VertexiumCypherException(String msg, Throwable e) {
        super(msg, e);
    }

    public VertexiumCypherException(String msg) {
        super(msg);
    }
}
