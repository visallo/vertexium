package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherUnaryExpression extends CypherExpression {
    private final Op op;
    private final CypherAstBase expression;

    public CypherUnaryExpression(Op op, CypherAstBase expression) {
        this.op = op;
        this.expression = expression;
    }

    public Op getOp() {
        return op;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(expression);
    }

    public enum Op {
        NOT
    }
}
