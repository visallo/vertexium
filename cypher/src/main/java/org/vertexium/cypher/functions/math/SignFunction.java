package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.executor.ExpressionScope;

public class SignFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value, ExpressionScope scope) {
        if (value == 0) {
            return 0;
        }
        if (value < 0) {
            return -1;
        }
        return 1;
    }

    @Override
    protected Object invokeLong(VertexiumCypherQueryContext ctx, long value, ExpressionScope scope) {
        if (value == 0) {
            return 0;
        }
        if (value < 0) {
            return -1;
        }
        return 1;
    }
}
