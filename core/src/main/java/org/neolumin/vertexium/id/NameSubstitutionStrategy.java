package org.neolumin.vertexium.id;

public interface NameSubstitutionStrategy {
    public String deflate(String value);

    public String inflate(String value);
}
