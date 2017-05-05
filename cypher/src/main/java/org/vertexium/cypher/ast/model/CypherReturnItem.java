package org.vertexium.cypher.ast.model;

import org.vertexium.VertexiumException;

import java.util.stream.Stream;

public class CypherReturnItem extends CypherAstBase {
    private final CypherAstBase expression;
    private final String alias;
    private final String originalText;

    public CypherReturnItem(String originalText, CypherAstBase expression, String alias) {
        this.originalText = originalText;
        if (expression == null && alias == null) {
            throw new VertexiumException("both expression and alias cannot be null");
        }
        this.expression = expression;
        this.alias = alias;
    }

    public String getOriginalText() {
        return originalText;
    }

    public CypherAstBase getExpression() {
        return expression;
    }

    public String getAlias() {
        return alias;
    }

    public String getResultColumnName() {
        if (getAlias() != null) {
            return getAlias();
        }
        return getOriginalText();
    }

    @Override
    public String toString() {
        if (getAlias() != null) {
            return getExpression() + " AS " + getAlias();
        } else {
            return getExpression().toString();
        }
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(expression);
    }
}
