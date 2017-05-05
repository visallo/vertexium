package org.vertexium.cypher.ast.model;

public class CypherSetVariable extends CypherSetItem<CypherVariable, CypherAstBase> {
    public CypherSetVariable(CypherVariable cypherVariable, Op op, CypherAstBase value) {
        super(cypherVariable, op, value);
    }
}
