package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.executor.ExpressionScope;

import java.time.ZonedDateTime;

public class MonthFunction extends CypherUnaryDateFunction {
    @Override
    protected Object invokeZonedDateTime(VertexiumCypherQueryContext ctx, ZonedDateTime date, ExpressionScope scope) {
        return date.getMonth();
    }
}
