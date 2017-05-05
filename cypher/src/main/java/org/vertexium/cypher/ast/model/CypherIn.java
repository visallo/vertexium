package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherIn extends CypherAstBase {
    private final CypherAstBase valueExpression;
    private final CypherAstBase arrayExpression;

    public CypherIn(CypherAstBase valueExpression, CypherAstBase arrayExpression) {
        this.valueExpression = valueExpression;
        this.arrayExpression = arrayExpression;
    }

    public CypherAstBase getValueExpression() {
        return valueExpression;
    }

    public CypherAstBase getArrayExpression() {
        return arrayExpression;
    }

    @Override
    public String toString() {
        return String.format("%s IN %s", getValueExpression(), getArrayExpression());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(valueExpression, arrayExpression);
    }
}
