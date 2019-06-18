package org.vertexium.cypher.functions.string;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class SubstringFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 2, 3);
        Object originalObj = arguments[0];
        Object startObj = arguments[1];
        Object lengthObj = arguments.length > 2 ? arguments[2] : null;

        int start;
        if (startObj instanceof Number) {
            start = ((Number) startObj).intValue();
        } else {
            throw new VertexiumCypherTypeErrorException(startObj, Number.class);
        }

        Integer length;
        if (lengthObj == null) {
            length = null;
        } else if (lengthObj instanceof Number) {
            length = ((Number) lengthObj).intValue();
        } else {
            throw new VertexiumCypherTypeErrorException(lengthObj, Number.class, null);
        }

        if (originalObj instanceof String) {
            String original = (String) originalObj;
            String result = original.substring(start);
            if (length != null) {
                result = result.substring(length);
            }
            return result;
        }

        throw new VertexiumCypherTypeErrorException(originalObj, String.class);
    }
}
