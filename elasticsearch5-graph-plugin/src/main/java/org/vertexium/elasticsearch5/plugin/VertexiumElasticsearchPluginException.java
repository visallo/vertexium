package org.vertexium.elasticsearch5.plugin;

public class VertexiumElasticsearchPluginException extends RuntimeException {
    private static final long serialVersionUID = 365430688799963961L;

    public VertexiumElasticsearchPluginException(String message) {
        super(message);
    }

    public VertexiumElasticsearchPluginException(String message, Throwable cause) {
        super(message, cause);
    }
}
