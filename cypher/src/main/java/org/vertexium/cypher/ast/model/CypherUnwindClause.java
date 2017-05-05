package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherUnwindClause extends CypherClause {
    private final String name;
    private final CypherAstBase expression;

    public CypherUnwindClause(String name, CypherAstBase expression) {
        this.name = name;
        this.expression = expression;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    public String getName() {
        return name;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(expression);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("UNWIND ").append(getExpression());
        if (getName() != null) {
            result.append(" AS ").append(getName());
        }
        return result.toString();
    }
}
