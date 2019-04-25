package org.vertexium.cypher.functions.list;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class TailFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 instanceof Stream) {
            return ((Stream) arg0).skip(1);
        }

        if (arg0 instanceof List) {
            List list = (List) arg0;
            return list.subList(1, list.size());
        }

        throw new VertexiumCypherTypeErrorException(arg0, Collection.class, Stream.class);
    }
}
