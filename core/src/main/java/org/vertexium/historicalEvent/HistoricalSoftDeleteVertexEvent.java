package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;

import java.time.ZonedDateTime;

public class HistoricalSoftDeleteVertexEvent extends HistoricalSoftDeleteElementEvent {
    public HistoricalSoftDeleteVertexEvent(
        String id,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.VERTEX, id, timestamp, data, fetchHints);
    }

    @Override
    public String toString() {
        return String.format("%s, data=%s}", super.toString(), getData());
    }
}
