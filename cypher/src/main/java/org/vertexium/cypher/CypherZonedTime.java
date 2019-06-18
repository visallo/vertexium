package org.vertexium.cypher;

import org.vertexium.cypher.exceptions.VertexiumCypherException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;

public class CypherZonedTime {
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
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm")
    };
    private final String value;

    public CypherZonedTime(String value) {
        this.value = value.replaceFirst("\\[.*\\]$", "");
    }

    public Temporal toZonedDateTime(ZonedDateTime startOfDay) {
        String str = startOfDay.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + "T" + value;
        for (DateTimeFormatter pattern : PATTERNS) {
            try {
                return ZonedDateTime.parse(str, pattern.withZone(startOfDay.getZone()));
            } catch (DateTimeParseException ex) {
                // Try the next one
            }
        }
        throw new VertexiumCypherException("Could not parse time: " + value);
    }
}
