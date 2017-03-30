package org.vertexium.cypher.executor.models.match;

import org.vertexium.cypher.ast.model.CypherAstBase;

import java.util.List;
import java.util.Set;

public class MatchConstraints {
    private final Set<PatternPartMatchConstraint> patternPartMatchConstraints;
    private final List<CypherAstBase> whereExpressions;

    public MatchConstraints(Set<PatternPartMatchConstraint> patternPartMatchConstraints, List<CypherAstBase> whereExpressions) {
        this.patternPartMatchConstraints = patternPartMatchConstraints;
        this.whereExpressions = whereExpressions;
    }

    public Set<PatternPartMatchConstraint> getPatternPartMatchConstraints() {
        return patternPartMatchConstraints;
    }

    public List<CypherAstBase> getWhereExpressions() {
        return whereExpressions;
    }
}
