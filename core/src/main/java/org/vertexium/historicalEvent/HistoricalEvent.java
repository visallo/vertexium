package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public abstract class HistoricalEvent implements Comparable<HistoricalEvent> {
    private final ElementType elementType;
    private final String elementId;
    private final ZonedDateTime timestamp;
    private final HistoricalEventsFetchHints fetchHints;

    public HistoricalEvent(
        ElementType elementType,
        String elementId,
        ZonedDateTime timestamp,
        HistoricalEventsFetchHints fetchHints
    ) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.timestamp = timestamp;
        this.fetchHints = fetchHints;
    }

    public static ZonedDateTime zonedDateTimeFromTimestamp(long epochMilli) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
    }

    public ElementType getElementType() {
        return elementType;
    }

    public String getElementId() {
        return elementId;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public HistoricalEventsFetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    public int compareTo(HistoricalEvent otherEvent) {
        return getHistoricalEventId().compareTo(otherEvent.getHistoricalEventId());
    }

    @Override
    public String toString() {
        return String.format(
            "%s {elementType=%s, elementId='%s', timestamp=%s",
            getClass().getSimpleName(),
            getElementType(),
            getElementId(),
            getTimestamp()
        );
    }

    public HistoricalEventId getHistoricalEventId() {
        return new HistoricalEventId(getTimestamp(), getElementType(), getElementId(), getHistoricalEventIdSubOrder());
    }

    protected String getHistoricalEventIdSubOrder() {
        return "~";
    }
}
