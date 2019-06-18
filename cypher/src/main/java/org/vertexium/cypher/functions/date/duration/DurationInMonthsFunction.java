package org.vertexium.cypher.functions.date.duration;

import org.vertexium.cypher.CypherDuration;
import org.vertexium.cypher.VertexiumCypherQueryContext;

import java.time.temporal.ChronoUnit;

public class DurationInMonthsFunction extends DurationBetweenFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        CypherDuration duration = (CypherDuration) super.executeFunction(ctx, arguments);
        if (duration == null) {
            return null;
        }
        return duration.truncatedTo(ChronoUnit.MONTHS);
    }
}
