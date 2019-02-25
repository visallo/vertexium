package org.vertexium.cypher.functions;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherArgumentErrorException;
import org.vertexium.cypher.executor.ExpressionScope;

import java.util.Arrays;
import java.util.stream.Collectors;

public abstract class CypherFunction {
    public void compile(CypherCompilerContext compilerContext, CypherAstBase[] arguments) {

    }

    public abstract Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope);

    protected void assertArgumentCount(CypherAstBase[] arguments, int... expectedCounts) {
        for (int count : expectedCounts) {
            if (arguments.length == count) {
                return;
            }
        }

        throw new VertexiumCypherArgumentErrorException(String.format(
            "Unexpected number of arguments. Expected %s, found %d",
            Arrays.stream(expectedCounts).mapToObj(Integer::toString).collect(Collectors.joining(", ")),
            arguments.length
        ));
    }

    public boolean isConstant() {
        return true;
    }
}
