package org.vertexium.cypher.ast.model;

public class CypherRelTypeName extends CypherLiteral<String> {
    public CypherRelTypeName(String value) {
        super(value);
    }

    @Override
    public String toString() {
        return ":" + getValue();
    }
}
