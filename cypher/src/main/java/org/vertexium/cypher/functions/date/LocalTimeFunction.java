package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherException;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalTimeFunction extends DateParseFunction {
    private static final DateTimeFormatter[] PATTERNS = new DateTimeFormatter[]{
        DateTimeFormatter.ofPattern("HH:mm"),
        DateTimeFormatter.ofPattern("HH:mm:ss"),
        DateTimeFormatter.ofPattern("HH:mm:ss.S"),
        DateTimeFormatter.ofPattern("HH:mm:ss.SS"),
        DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
    };

    @Override
    protected Object parseString(VertexiumCypherQueryContext ctx, String str) {
        for (DateTimeFormatter pattern : PATTERNS) {
            try {
                return LocalTime.parse(str, pattern.withZone(ctx.getZoneId()));
            } catch (DateTimeParseException ex) {
                // Try the next one
            }
        }
        throw new VertexiumCypherException("Could not parse time: " + str);
    }
}
