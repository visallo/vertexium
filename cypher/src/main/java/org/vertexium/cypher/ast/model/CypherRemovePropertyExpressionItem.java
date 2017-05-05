package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherRemovePropertyExpressionItem extends CypherRemoveItem {
    private final CypherAstBase propertyExpression;

    public CypherRemovePropertyExpressionItem(CypherAstBase propertyExpression) {
        this.propertyExpression = propertyExpression;
    }

    public CypherAstBase getPropertyExpression() {
        return propertyExpression;
    }

    @Override
    public String toString() {
        return getPropertyExpression().toString();
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(propertyExpression);
    }
}
