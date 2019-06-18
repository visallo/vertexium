package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.stream.Stream;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class SizeFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg = arguments[0];

        if (arg instanceof Collection) {
            return ((Collection) arg).size();
        }

        if (arg instanceof Stream) {
            return ((Stream) arg).count();
        }

        if (arg != null && arg.getClass().isArray()) {
            return Array.getLength(arg);
        }

        throw new VertexiumCypherTypeErrorException(arg, Collection.class, Stream.class, Object[].class);
    }
}
