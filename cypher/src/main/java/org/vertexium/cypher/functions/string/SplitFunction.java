package org.vertexium.cypher.functions.string;

import com.google.common.collect.Lists;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class SplitFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 2);
        Object stringArgObj = arguments[0];
        Object delimiterArgObj = arguments[1];

        if (!(stringArgObj instanceof String)) {
            throw new VertexiumException("Expected a string as the first argument, found " + stringArgObj.getClass().getName());
        }
        String stringArg = (String) stringArgObj;

        if (!(delimiterArgObj instanceof String)) {
            throw new VertexiumException("Expected a string as the second argument, found " + stringArgObj.getClass().getName());
        }
        String delimiterArg = (String) delimiterArgObj;

        return Lists.newArrayList(stringArg.split(delimiterArg));
    }
}
