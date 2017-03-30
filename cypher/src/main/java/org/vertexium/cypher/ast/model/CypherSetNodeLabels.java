package org.vertexium.cypher.ast.model;

public class CypherSetNodeLabels extends CypherSetItem<CypherVariable, CypherListLiteral<CypherLabelName>> {
    public CypherSetNodeLabels(CypherVariable cypherVariable, CypherListLiteral<CypherLabelName> cypherLabelNames) {
        super(cypherVariable, CypherSetItem.Op.PLUS_EQUAL, cypherLabelNames);
    }
}

