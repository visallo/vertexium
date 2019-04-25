package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.CypherZonedTime;
import org.vertexium.cypher.VertexiumCypherQueryContext;

public class TimeFunction extends DateParseFunction {
    @Override
    protected Object parseString(VertexiumCypherQueryContext ctx, String str) {
        return new CypherZonedTime(str);
    }
}
