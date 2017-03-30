package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public abstract class CypherAstBase {
    public abstract Stream<? extends CypherAstBase> getChildren();
}
