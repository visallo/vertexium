package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public abstract class HistoricalDeleteElementEvent extends HistoricalEvent {
    private final Visibility visibility;

    public HistoricalDeleteElementEvent(
        ElementType elementType,
        String elementId,
        Visibility visibility,
        ZonedDateTime timestamp,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, elementId, timestamp, fetchHints);
        this.visibility = visibility;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return String.format("%s, visibility=%s", super.toString(), getVisibility());
    }

    @Override
    protected int getHistoricalEventIdSubOrder() {
        return 0;
    }
}
