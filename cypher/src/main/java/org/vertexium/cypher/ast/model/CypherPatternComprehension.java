package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherPatternComprehension extends CypherAstBase {
    private final CypherMatchClause matchClause;
    private final CypherAstBase expression;

    public CypherPatternComprehension(
            CypherMatchClause matchClause,
            CypherAstBase expression
    ) {
        this.matchClause = matchClause;
        this.expression = expression;
    }

    public CypherMatchClause getMatchClause() {
        return matchClause;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("[");
        result.append(getMatchClause());
        result.append(" | ");
        result.append(getExpression());
        result.append("]");
        return result.toString();
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(
                matchClause,
                expression
        );
    }
}
