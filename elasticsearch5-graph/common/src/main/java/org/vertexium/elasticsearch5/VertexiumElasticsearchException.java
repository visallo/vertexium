package org.vertexium.elasticsearch5;

public class VertexiumElasticsearchException extends RuntimeException {
    private static final long serialVersionUID = 365430688799963961L;

    public VertexiumElasticsearchException(String message) {
        super(message);
    }

    public VertexiumElasticsearchException(String message, Throwable cause) {
        super(message, cause);
    }
}
