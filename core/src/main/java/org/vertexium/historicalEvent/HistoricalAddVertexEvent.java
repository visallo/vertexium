package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalAddVertexEvent extends HistoricalAddElementEvent {
    public HistoricalAddVertexEvent(
        String id,
        Visibility createVisibility,
        ZonedDateTime createTimestamp,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.VERTEX, id, createVisibility, createTimestamp, fetchHints);
    }

    @Override
    public String toString() {
        return String.format("%s}", super.toString());
    }
}
