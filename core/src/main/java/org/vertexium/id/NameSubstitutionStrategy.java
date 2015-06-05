package org.vertexium.id;

public interface NameSubstitutionStrategy {
    String deflate(String value);

    String inflate(String value);
}
