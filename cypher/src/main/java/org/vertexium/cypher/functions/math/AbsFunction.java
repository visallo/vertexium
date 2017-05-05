package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.executor.ExpressionScope;

public class AbsFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value, ExpressionScope scope) {
        return Math.abs(value);
    }

    @Override
    protected Object invokeLong(VertexiumCypherQueryContext ctx, long value, ExpressionScope scope) {
        return Math.abs(value);
    }
}
