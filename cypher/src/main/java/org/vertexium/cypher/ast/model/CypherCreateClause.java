package org.vertexium.cypher.ast.model;

import com.google.common.collect.ImmutableList;
import org.vertexium.cypher.exceptions.VertexiumCypherSyntaxErrorException;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherCreateClause extends CypherClause {
    private final ImmutableList<CypherPatternPart> patternParts;

    public CypherCreateClause(ImmutableList<CypherPatternPart> patternParts) {
        checkRequiresDirectedRelationship(patternParts);
        this.patternParts = patternParts;
    }

    private void checkRequiresDirectedRelationship(ImmutableList<CypherPatternPart> patternParts) {
        for (CypherPatternPart patternPart : patternParts) {
            for (CypherElementPattern elementPattern : patternPart.getElementPatterns()) {
                if (elementPattern instanceof CypherRelationshipPattern) {
                    CypherDirection direction = ((CypherRelationshipPattern) elementPattern).getDirection();
                    if (!direction.isDirected()) {
                        throw new VertexiumCypherSyntaxErrorException("RequiresDirectedRelationship: Relationship create statements must have direction: " + elementPattern);
                    }
                }
            }
        }
    }

    public ImmutableList<CypherPatternPart> getPatternParts() {
        return patternParts;
    }

    @Override
    public String toString() {
        return String.format(
            "CREATE %s",
            getPatternParts().stream().map(CypherPatternPart::toString).collect(Collectors.joining(", "))
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return patternParts.stream();
    }
}
