package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherTrueExpression extends CypherExpression {
    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.empty();
    }

    @Override
    public String toString() {
        return "true";
    }
}
