package org.vertexium.cypher.ast.model;

public class CypherSetProperty extends CypherSetItem<CypherLookup, CypherAstBase> {
    public CypherSetProperty(CypherLookup lookup, CypherAstBase value) {
        super(lookup, Op.EQUAL, value);
    }
}
