package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;

public class NegateFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value) {
        return -value;
    }

    @Override
    protected Object invokeLong(VertexiumCypherQueryContext ctx, long value) {
        return -value;
    }
}
