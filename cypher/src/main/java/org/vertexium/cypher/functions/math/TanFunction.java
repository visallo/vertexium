package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;

public class TanFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value) {
        return Math.tan(value);
    }
}
