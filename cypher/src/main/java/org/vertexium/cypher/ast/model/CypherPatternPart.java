package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherPatternPart extends CypherAstBase {
    private final String name;
    private final CypherListLiteral<CypherElementPattern> elementPatterns;

    public CypherPatternPart(String name, CypherListLiteral<CypherElementPattern> elementPatterns) {
        this.name = name;
        this.elementPatterns = elementPatterns;
    }

    public String getName() {
        return name;
    }

    public CypherListLiteral<CypherElementPattern> getElementPatterns() {
        return elementPatterns;
    }

    @Override
    public String toString() {
        if (getName() != null) {
            return String.format("%s = %s", getName(), getElementPatterns().toString(""));
        } else {
            return getElementPatterns().toString("");
        }
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return elementPatterns.stream();
    }
}
