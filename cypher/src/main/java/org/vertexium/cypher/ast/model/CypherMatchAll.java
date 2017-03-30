package org.vertexium.cypher.ast.model;

public class CypherMatchAll extends CypherLiteral<String> {
    public CypherMatchAll() {
        super("*");
    }

    @Override
    public String toString() {
        return "*";
    }
}
