package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;

import java.time.ZonedDateTime;

public class HistoricalAlterEdgeLabelEvent extends HistoricalEvent {
    private final String newEdgeLabel;

    public HistoricalAlterEdgeLabelEvent(
        String id,
        String newEdgeLabel,
        ZonedDateTime timestamp,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.EDGE, id, timestamp, fetchHints);
        this.newEdgeLabel = newEdgeLabel;
    }

    public String getNewEdgeLabel() {
        return newEdgeLabel;
    }

    @Override
    public String toString() {
        return String.format("%s, newEdgeLabel='%s'}", super.toString(), getNewEdgeLabel());
    }
}
