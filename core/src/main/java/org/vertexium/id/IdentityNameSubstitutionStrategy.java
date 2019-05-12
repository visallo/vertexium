package org.vertexium.id;

import java.util.Map;

public class IdentityNameSubstitutionStrategy implements NameSubstitutionStrategy {
    @Override
    public void setup(Map config) {
    }

    @Override
    public String deflate(String value) {
        return value;
    }

    @Override
    public String inflate(String value) {
        return value;
    }
}
