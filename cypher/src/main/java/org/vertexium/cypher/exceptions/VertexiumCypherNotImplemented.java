package org.vertexium.cypher.exceptions;

public class VertexiumCypherNotImplemented extends VertexiumCypherException {
    private static final long serialVersionUID = -7342371148840304840L;

    public VertexiumCypherNotImplemented(String message) {
        super("not implemented: " + message);
    }
}
