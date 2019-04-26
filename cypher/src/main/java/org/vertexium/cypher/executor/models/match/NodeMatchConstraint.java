package org.vertexium.cypher.executor.models.match;

import org.vertexium.cypher.ast.model.CypherNodePattern;

import java.util.List;

public class NodeMatchConstraint extends MatchConstraint<CypherNodePattern, RelationshipMatchConstraint> {
    public NodeMatchConstraint(
        String name,
        List<CypherNodePattern> nodePatterns,
        boolean optional
    ) {
        super(name, nodePatterns, optional);
    }
}
