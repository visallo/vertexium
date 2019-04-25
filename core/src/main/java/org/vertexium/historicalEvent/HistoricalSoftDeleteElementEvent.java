package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;

import java.time.ZonedDateTime;

public abstract class HistoricalSoftDeleteElementEvent extends HistoricalEvent {
    private final Object data;

    public HistoricalSoftDeleteElementEvent(
        ElementType elementType,
        String id,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, id, timestamp, fetchHints);
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
