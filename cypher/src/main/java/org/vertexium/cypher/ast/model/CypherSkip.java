package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherSkip extends CypherAstBase {
    private final CypherAstBase expression;

    public CypherSkip(CypherAstBase expression) {
        this.expression = expression;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        return String.format("SKIP %s", getExpression());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(expression);
    }
}
