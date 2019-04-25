package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;

public class CotFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value) {
        return 1.0 / Math.tan(value);
    }
}
