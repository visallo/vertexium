package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;

public class AbsFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value) {
        return Math.abs(value);
    }

    @Override
    protected Object invokeLong(VertexiumCypherQueryContext ctx, long value) {
        return Math.abs(value);
    }
}
