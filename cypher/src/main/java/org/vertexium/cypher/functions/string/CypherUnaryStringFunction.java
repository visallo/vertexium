package org.vertexium.cypher.functions.string;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public abstract class CypherUnaryStringFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];
        if (arg0 == null) {
            return null;
        }
        if (arg0 instanceof String) {
            return invokeOnString((String) arg0);
        }
        throw new VertexiumCypherTypeErrorException(arg0, String.class, null);
    }

    protected abstract Object invokeOnString(String str);
}
