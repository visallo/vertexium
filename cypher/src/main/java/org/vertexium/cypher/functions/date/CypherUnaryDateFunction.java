package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

public abstract class CypherUnaryDateFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof Date) {
            Date d = (Date) arg0;
            return invokeZonedDateTime(ctx, ZonedDateTime.ofInstant(d.toInstant(), ZoneOffset.UTC), scope);
        }

        throw new VertexiumCypherTypeErrorException(arg0, Date.class);
    }

    protected abstract Object invokeZonedDateTime(VertexiumCypherQueryContext ctx, ZonedDateTime date, ExpressionScope scope);
}
