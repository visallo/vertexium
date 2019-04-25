package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public abstract class CypherUnaryDateFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof Date) {
            Date d = (Date) arg0;
            return invokeZonedDateTime(ctx, ZonedDateTime.ofInstant(d.toInstant(), ZoneOffset.UTC));
        }

        throw new VertexiumCypherTypeErrorException(arg0, Date.class);
    }

    protected abstract Object invokeZonedDateTime(VertexiumCypherQueryContext ctx, ZonedDateTime date);
}
