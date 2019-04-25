package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;

public class SquareRootFunction extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value) {
        return Math.sqrt(value);
    }
}
