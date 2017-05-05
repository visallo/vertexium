package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.executor.ExpressionScope;

public class TanFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value, ExpressionScope scope) {
        return Math.tan(value);
    }
}
