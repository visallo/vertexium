package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class IsNullFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        return arguments[0] == null;
    }
}
