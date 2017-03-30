package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherLiteralExpression extends CypherExpression {
    private final Object value;

    public CypherLiteralExpression(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        if (value instanceof CypherAstBase) {
            return Stream.of((CypherAstBase) value);
        }
        return Stream.empty();
    }
}
