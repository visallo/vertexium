package org.vertexium.cypher.functions.string;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class ReverseFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);

        Object arg0 = arguments[0];

        if (arg0 instanceof String) {
            return new StringBuilder((String) arg0).reverse().toString();
        }

        throw new VertexiumException("not implemented for argument of type " + arg0.getClass().getName());
    }
}
