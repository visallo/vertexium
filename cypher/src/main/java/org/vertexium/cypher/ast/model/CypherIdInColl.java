package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherIdInColl extends CypherAstBase {
    private final CypherVariable variable;
    private final CypherAstBase expression;

    public CypherIdInColl(CypherVariable variable, CypherAstBase expression) {
        this.variable = variable;
        this.expression = expression;
    }

    public CypherVariable getVariable() {
        return variable;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        return String.format("%s IN %s", getVariable(), getExpression());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(variable, expression);
    }
}
