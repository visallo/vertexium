package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherFunctionInvocation extends CypherAstBase {
    private final String functionName;
    private final boolean distinct;
    private final CypherAstBase[] arguments;

    public CypherFunctionInvocation(String functionName, boolean distinct, CypherAstBase... arguments) {
        this.functionName = functionName;
        this.distinct = distinct;
        this.arguments = arguments;
    }

    public String getFunctionName() {
        return functionName;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public CypherAstBase[] getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return String.format(
                "%s(%s%s)",
                getFunctionName(),
                isDistinct() ? "DISTINCT " : "",
                CypherExpression.toString(getArguments())
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(arguments);
    }
}
