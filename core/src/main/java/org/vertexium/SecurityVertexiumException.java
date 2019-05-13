package org.vertexium;

public class SecurityVertexiumException extends VertexiumException {
    private final User user;

    public SecurityVertexiumException(String message, User user) {
        super(message);
        this.user = user;
    }

    public SecurityVertexiumException(String message, User user, Throwable cause) {
        super(message, cause);
        this.user = user;
    }

    public User getUser() {
        return user;
    }
}
