package org.vertexium;

public class AuthorizationsUser implements User {
    private final Authorizations authorizations;

    public AuthorizationsUser(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    public String[] getAuthorizations() {
        return authorizations.getAuthorizations();
    }

    @Override
    public boolean canRead(Visibility visibility) {
        return authorizations.canRead(visibility);
    }

    @Override
    public String toString() {
        return String.format("AuthorizationsUser{authorizations=%s}", authorizations);
    }
}
