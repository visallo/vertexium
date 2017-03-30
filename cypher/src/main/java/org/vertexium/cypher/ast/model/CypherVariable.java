package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherVariable extends CypherAstBase {
    private final String name;

    public CypherVariable(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of();
    }
}
