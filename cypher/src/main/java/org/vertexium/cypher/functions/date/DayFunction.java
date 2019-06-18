package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;

import java.time.ZonedDateTime;

public class DayFunction extends CypherUnaryDateFunction {
    @Override
    protected Object invokeZonedDateTime(VertexiumCypherQueryContext ctx, ZonedDateTime date) {
        return date.getDayOfMonth();
    }
}
