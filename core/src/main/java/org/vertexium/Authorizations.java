package org.vertexium;

import java.io.Serializable;

public interface Authorizations extends Serializable {
    boolean canRead(Visibility visibility);

    String[] getAuthorizations();

    boolean equals(Authorizations authorizations);

    default User getUser() {
        return new AuthorizationsUser(this);
    }
}
