package org.vertexium.id;

public class IdentityNameSubstitutionStrategy implements NameSubstitutionStrategy {
    @Override
    public String deflate(String value) {
        return value;
    }

    @Override
    public String inflate(String value) {
        return value;
    }
}
