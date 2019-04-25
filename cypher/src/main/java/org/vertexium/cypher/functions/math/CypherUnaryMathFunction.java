package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public abstract class CypherUnaryMathFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 instanceof Double || arg0 instanceof Float) {
            return invokeDouble(ctx, ((Number) arg0).doubleValue());
        }

        if (arg0 instanceof Integer || arg0 instanceof Long) {
            return invokeLong(ctx, ((Number) arg0).longValue());
        }

        throw new VertexiumCypherTypeErrorException(arg0, Double.class, Float.class, Integer.class, Long.class);
    }

    protected Object invokeLong(VertexiumCypherQueryContext ctx, long value) {
        return invokeDouble(ctx, (double) value);
    }

    protected abstract Object invokeDouble(VertexiumCypherQueryContext ctx, double value);
}
