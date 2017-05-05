package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherIsNotNull extends CypherAstBase {
    private final CypherAstBase valueExpression;

    public CypherIsNotNull(CypherAstBase valueExpression) {
        this.valueExpression = valueExpression;
    }

    public CypherAstBase getValueExpression() {
        return valueExpression;
    }

    @Override
    public String toString() {
        return String.format("%s IS NOT NULL", getValueExpression());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(valueExpression);
    }
}
