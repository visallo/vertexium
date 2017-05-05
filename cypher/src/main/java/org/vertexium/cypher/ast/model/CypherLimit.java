package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherLimit extends CypherAstBase {
    private final CypherAstBase expression;

    public CypherLimit(CypherAstBase expression) {
        this.expression = expression;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        return String.format("LIMIT %s", getExpression());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(expression);
    }
}
