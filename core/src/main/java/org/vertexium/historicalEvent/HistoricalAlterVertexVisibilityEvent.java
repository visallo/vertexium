package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalAlterVertexVisibilityEvent extends HistoricalAlterElementVisibilityEvent {
    public HistoricalAlterVertexVisibilityEvent(
        String elementId,
        Visibility oldVisibility,
        Visibility newVisibility,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.VERTEX, elementId, oldVisibility, newVisibility, timestamp, data, fetchHints);
    }

    @Override
    public String toString() {
        return String.format("%s}", super.toString());
    }
}
