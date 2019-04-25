package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherSortItem extends CypherAstBase {
    private final CypherAstBase expression;
    private final Direction direction;
    private final String expressionText;

    public CypherSortItem(CypherAstBase expression, Direction direction, String expressionText) {
        this.expression = expression;
        this.direction = direction;
        this.expressionText = expressionText;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getExpressionText() {
        return expressionText;
    }

    @Override
    public String toString() {
        return String.format(
            "%s %s",
            getExpression(),
            getDirection().name()
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(expression);
    }

    public enum Direction {
        DESCENDING, ASCENDING
    }
}
