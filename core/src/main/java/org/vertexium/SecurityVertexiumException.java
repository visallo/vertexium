package org.vertexium;

public class SecurityVertexiumException extends VertexiumException {
    private final Authorizations authorizations;

    public SecurityVertexiumException(String message, Authorizations authorizations) {
        super(message);
        this.authorizations = authorizations;
    }

    public SecurityVertexiumException(String message, Authorizations authorizations, Throwable cause) {
        super(message, cause);
        this.authorizations = authorizations;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }
}
