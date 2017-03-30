package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherComparisonExpression extends CypherExpression {
    private final CypherAstBase left;
    private final String op;
    private final CypherAstBase right;

    public CypherComparisonExpression(CypherAstBase left, String op, CypherAstBase right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    public CypherAstBase getLeft() {
        return left;
    }

    public String getOp() {
        return op;
    }

    public CypherAstBase getRight() {
        return right;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", getLeft(), getOp(), getRight());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(left, right);
    }
}
