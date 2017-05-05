package org.vertexium.cypher.exceptions;

public class VertexiumCypherSyntaxErrorException extends VertexiumCypherException {
    private static final long serialVersionUID = 3262990761198793941L;

    public VertexiumCypherSyntaxErrorException(String message) {
        super(message);
    }

    public VertexiumCypherSyntaxErrorException(String message, Throwable cause) {
        super(message, cause);
    }
}
