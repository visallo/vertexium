package org.vertexium.cypher.functions.date;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateTimeFunction extends DateParseFunction {
    private static final DateTimeFormatter[] PATTERNS = new DateTimeFormatter[]{
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmXXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:XXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SXXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSXXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:XX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX")
    };

    @Override
    protected Object parseString(VertexiumCypherQueryContext ctx, String str) {
        str = str.replaceFirst("\\[.*\\]$", "");
        for (DateTimeFormatter pattern : PATTERNS) {
            try {
                return ZonedDateTime.parse(str, pattern);
            } catch (DateTimeParseException ex) {
                // Try the next one
            }
        }
        throw new VertexiumCypherException("Could not parse date: " + str);
    }
}
