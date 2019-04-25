package org.vertexium.cypher.functions.scalar;

import org.vertexium.Edge;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class TypeFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof Edge) {
            Edge arg0Edge = (Edge) arg0;
            return arg0Edge.getLabel();
        }

        throw new VertexiumCypherTypeErrorException(arg0, Edge.class, null);
    }
}
