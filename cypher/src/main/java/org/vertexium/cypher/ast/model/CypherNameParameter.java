package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherNameParameter extends CypherParameter {
    private final String parameterName;

    public CypherNameParameter(String parameterName) {
        this.parameterName = parameterName;
    }

    public String getParameterName() {
        return parameterName;
    }

    @Override
    public String toString() {
        return String.format("$%s", getParameterName());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.empty();
    }
}
