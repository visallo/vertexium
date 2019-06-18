package org.vertexium.cypher.functions.string;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class ContainsFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 2);
        Object arg0 = arguments[0];
        Object arg1 = arguments[1];

        if (arg1 == null || !(arg0 instanceof String) || !(arg1 instanceof String)) {
            return null;
        }

        if (arg0 == null) {
            return false;
        }

        return ((String) arg0).indexOf((String) arg1) >= 0;
    }
}
