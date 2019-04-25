package org.vertexium.cypher.ast.model;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherMatchClause extends CypherClause {
    private final CypherListLiteral<CypherPatternPart> patternParts;
    private final CypherAstBase whereExpression;
    private final boolean optional;

    public CypherMatchClause(
        boolean optional,
        CypherListLiteral<CypherPatternPart> patternParts,
        CypherAstBase whereExpression
    ) {
        this.optional = optional;
        this.patternParts = patternParts;
        this.whereExpression = whereExpression;
    }

    public boolean isOptional() {
        return optional;
    }

    public CypherListLiteral<CypherPatternPart> getPatternParts() {
        return patternParts;
    }

    public CypherAstBase getWhereExpression() {
        return whereExpression;
    }

    @Override
    public String toString() {
        return String.format(
            "%sMATCH %s%s",
            isOptional() ? "OPTIONAL " : "",
            getPatternParts().stream().map(CypherPatternPart::toString).collect(Collectors.joining(", ")),
            getWhereExpression() == null ? "" : " WHERE " + getWhereExpression()
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        if (whereExpression != null) {
            return Stream.concat(
                patternParts.stream(),
                Stream.of(whereExpression)
            );
        } else {
            return patternParts.stream();
        }
    }
}
