package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class HeadFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 instanceof Stream) {
            Stream<?> stream = (Stream<?>) arg0;
            return stream.findFirst().orElse(null);
        }

        if (arg0 instanceof List) {
            List list = (List) arg0;
            if (list.size() == 0) {
                return null;
            }
            return list.get(0);
        }

        if (arg0 instanceof Collection) {
            Collection collection = (Collection) arg0;
            if (collection.size() == 0) {
                return null;
            }
            return collection.iterator().next();
        }

        if (arg0.getClass().isArray()) {
            if (Array.getLength(arg0) == 0) {
                return null;
            }
            return Array.get(arg0, 0);
        }

        throw new VertexiumCypherTypeErrorException(arg0, Collection.class, Stream.class);
    }
}
