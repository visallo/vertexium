package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class ATan2Function extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 2);
        Object arg0 = arguments[0];
        Object arg1 = arguments[1];
        if (!(arg0 instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(arg0, Number.class);
        }
        if (!(arg1 instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(arg1, Number.class);
        }
        return Math.atan2(((Number) arg0).doubleValue(), ((Number) arg1).doubleValue());
    }
}
