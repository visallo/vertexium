package org.vertexium.cypher.ast.model;

public class CypherInteger extends CypherLiteral<Long> {
    public CypherInteger(Long value) {
        super(value);
    }

    public int getIntValue() {
        return getValue().intValue();
    }
}
