package org.vertexium.cypher.ast.model;

import java.util.List;
import java.util.stream.Stream;

public class CypherMergeClause extends CypherClause {
    private final CypherPatternPart patternPart;
    private final List<CypherMergeAction> mergeActions;

    public CypherMergeClause(CypherPatternPart patternPart, List<CypherMergeAction> mergeActions) {
        this.patternPart = patternPart;
        this.mergeActions = mergeActions;
    }

    public CypherPatternPart getPatternPart() {
        return patternPart;
    }

    public List<CypherMergeAction> getMergeActions() {
        return mergeActions;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.concat(
            Stream.of(getPatternPart()),
            getMergeActions().stream()
        );
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("MERGE ");
        result.append(getPatternPart());
        for (CypherMergeAction mergeAction : getMergeActions()) {
            result.append(" ");
            result.append(mergeAction.toString());
        }
        return result.toString();
    }
}
