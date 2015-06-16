package org.vertexium.id;

import java.util.Map;

public interface NameSubstitutionStrategy {
    void setup(Map config);

    String deflate(String value);

    String inflate(String value);
}
