package org.vertexium.cypher.ast.model;

public class CypherMergeActionCreate extends CypherMergeAction {
    public CypherMergeActionCreate(CypherSetClause set) {
        super(set);
    }

    @Override
    public String toString() {
        return "ON CREATE " + getSet();
    }
}
