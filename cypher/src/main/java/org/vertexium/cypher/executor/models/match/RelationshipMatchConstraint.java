package org.vertexium.cypher.executor.models.match;

import org.vertexium.cypher.ast.model.CypherDirection;
import org.vertexium.cypher.ast.model.CypherNodePattern;
import org.vertexium.cypher.ast.model.CypherRelationshipPattern;

import java.util.List;

public class RelationshipMatchConstraint extends MatchConstraint<CypherRelationshipPattern, NodeMatchConstraint> {
    public RelationshipMatchConstraint(
            String name,
            List<CypherRelationshipPattern> relationshipPatterns,
            boolean optional
    ) {
        super(name, relationshipPatterns, optional);
    }

    public CypherDirection getDirection() {
        CypherDirection result = null;
        for (CypherRelationshipPattern relationshipPattern : getPatterns()) {
            if (result == null) {
                result = relationshipPattern.getDirection();
            } else {
                result = result.merge(relationshipPattern.getDirection());
            }
        }
        return result;
    }

    public RelationshipMatchRange getRange() {
        RelationshipMatchRange range = null;
        for (CypherRelationshipPattern relationshipPattern : getPatterns()) {
            if (range == null) {
                range = new RelationshipMatchRange(relationshipPattern);
            } else {
                range = range.merge(relationshipPattern);
            }
        }
        return range;
    }

    public boolean isFoundInPrevious(NodeMatchConstraint constraint) {
        return getPatterns().stream()
                .anyMatch(relationshipPattern -> {
                    CypherNodePattern previousNodePattern = relationshipPattern.getPreviousNodePattern();
                    return constraint.getPatterns().stream()
                            .anyMatch(o -> o == previousNodePattern);
                });
    }

    public boolean isFoundInNext(NodeMatchConstraint constraint) {
        return getPatterns().stream()
                .anyMatch(relationshipPattern -> {
                    CypherNodePattern nextNodePattern = relationshipPattern.getNextNodePattern();
                    return constraint.getPatterns().stream()
                            .anyMatch(o -> o == nextNodePattern);
                });
    }
}
