package org.vertexium;

public interface User {
    String[] getAuthorizations();

    boolean canRead(Visibility visibility);
}
