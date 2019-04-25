package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;

public class Log10Function extends CypherUnaryMathFunction {
    @Override
    protected Object invokeDouble(VertexiumCypherQueryContext ctx, double value) {
        return Math.log10(value);
    }
}
