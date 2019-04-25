package org.vertexium.cypher.functions;

import org.vertexium.cypher.VertexiumCypherQueryContext;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public abstract class TypeConversionFunction extends SimpleCypherFunction {
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        return convert(arguments[0]);
    }

    protected abstract Object convert(Object value);
}
