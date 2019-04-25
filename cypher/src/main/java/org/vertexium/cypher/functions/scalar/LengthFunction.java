package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.PathResultBase;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class LengthFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 instanceof Collection) {
            arg0 = ((Collection) arg0).stream();
        }

        if (arg0 instanceof Stream) {
            return ((Stream<?>) arg0)
                .map(this::getLength)
                .collect(Collectors.toList());
        }

        return getLength(arg0);
    }

    private Object getLength(Object arg0) {
        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof PathResultBase) {
            return ((PathResultBase) arg0).getLength();
        }

        if (arg0 instanceof String) {
            return ((String) arg0).length();
        }

        if (arg0.getClass().isArray()) {
            return Array.getLength(arg0);
        }

        throw new VertexiumCypherTypeErrorException(arg0, PathResultBase.class, String.class, Object[].class, null);
    }
}
