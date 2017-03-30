package org.vertexium.cypher.ast.model;

import org.vertexium.VertexiumException;

import java.util.stream.Stream;

public class CypherStringMatch extends CypherAstBase {
    private final CypherAstBase valueExpression;
    private final CypherAstBase stringExpression;
    private final Op op;

    public CypherStringMatch(CypherAstBase valueExpression, CypherAstBase stringExpression, Op op) {
        this.valueExpression = valueExpression;
        this.stringExpression = stringExpression;
        this.op = op;
    }

    public CypherAstBase getValueExpression() {
        return valueExpression;
    }

    public CypherAstBase getStringExpression() {
        return stringExpression;
    }

    public Op getOp() {
        return op;
    }

    @Override
    public String toString() {
        switch (getOp()) {
            case STARTS_WITH:
                return String.format("%s STARTS WITH %s", getValueExpression(), getStringExpression());
            case ENDS_WITH:
                return String.format("%s ENDS WITH %s", getValueExpression(), getStringExpression());
            case CONTAINS:
                return String.format("%s CONTAINS %s", getValueExpression(), getStringExpression());
            default:
                throw new VertexiumException("unhandled: " + getOp());
        }
    }

    public enum Op {
        STARTS_WITH,
        ENDS_WITH,
        CONTAINS
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(valueExpression, stringExpression);
    }
}
