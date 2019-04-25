package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class EFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 0);
        return Math.E;
    }
}
