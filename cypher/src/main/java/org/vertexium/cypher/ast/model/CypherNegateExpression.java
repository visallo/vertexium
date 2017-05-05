package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherNegateExpression extends CypherExpression {
    private final CypherAstBase value;

    public CypherNegateExpression(CypherAstBase value) {
        this.value = value;
    }

    public CypherAstBase getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("-%s", getValue());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(value);
    }
}
