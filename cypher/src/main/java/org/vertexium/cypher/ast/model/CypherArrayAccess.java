package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherArrayAccess extends CypherAstBase {
    private final CypherAstBase arrayExpression;
    private final CypherAstBase indexExpression;

    public CypherArrayAccess(CypherAstBase arrayExpression, CypherAstBase indexExpression) {
        this.arrayExpression = arrayExpression;
        this.indexExpression = indexExpression;
    }

    public CypherAstBase getArrayExpression() {
        return arrayExpression;
    }

    public CypherAstBase getIndexExpression() {
        return indexExpression;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", getArrayExpression(), getIndexExpression());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(arrayExpression, indexExpression);
    }
}
