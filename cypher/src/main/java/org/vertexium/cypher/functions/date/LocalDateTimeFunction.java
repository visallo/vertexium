package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalDateTimeFunction extends DateParseFunction {
    private static final DateTimeFormatter[] PATTERNS = new DateTimeFormatter[]{
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SS"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    };

    @Override
    protected Object parseString(VertexiumCypherQueryContext ctx, String str) {
        for (DateTimeFormatter pattern : PATTERNS) {
            try {
                return ZonedDateTime.parse(str, pattern.withZone(ctx.getZoneId()));
            } catch (DateTimeParseException ex) {
                // Try the next one
            }
        }
        throw new VertexiumCypherException("Could not parse date: " + str);
    }
}
