package org.vertexium;

public class AuthorizationsUser implements User {
    private final Authorizations authorizations;

    public AuthorizationsUser(Authorizations authorizations) {
        this.authorizations = authorizations;
    }
}
