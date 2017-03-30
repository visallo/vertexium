package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherListComprehension extends CypherAstBase {
    private final CypherFilterExpression filterExpression;
    private final CypherAstBase expression;

    public CypherListComprehension(CypherFilterExpression filterExpression, CypherAstBase expression) {
        this.filterExpression = filterExpression;
        this.expression = expression;
    }

    public CypherFilterExpression getFilterExpression() {
        return filterExpression;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        if (getExpression() != null) {
            return String.format("[%s | %s]", getFilterExpression(), getExpression());
        } else {
            return String.format("[%s]", getFilterExpression());
        }
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(filterExpression, expression);
    }
}
