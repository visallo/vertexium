package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateFunction extends DateParseFunction {
    private static final DateTimeFormatter[] PATTERNS = new DateTimeFormatter[]{
        DateTimeFormatter.ofPattern("yyyy-MM-dd")
    };

    @Override
    protected Object parseString(VertexiumCypherQueryContext ctx, String str) {
        for (DateTimeFormatter pattern : PATTERNS) {
            try {
                return LocalDate.parse(str, pattern).atStartOfDay(ctx.getZoneId());
            } catch (DateTimeParseException ex) {
                // Try the next one
            }
        }
        throw new VertexiumCypherException("Could not parse date: " + str);
    }
}
