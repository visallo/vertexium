package org.vertexium.historicalEvent;

import org.vertexium.Direction;
import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalAddEdgeToVertexEvent extends HistoricalEvent {
    private final String edgeId;
    private final Direction edgeDirection;
    private final String edgeLabel;
    private final String otherVertexId;
    private final Visibility edgeVisibility;

    public HistoricalAddEdgeToVertexEvent(
        String elementId,
        String edgeId,
        Direction edgeDirection,
        String edgeLabel,
        String otherVertexId,
        Visibility edgeVisibility,
        ZonedDateTime timestamp,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.VERTEX, elementId, timestamp, fetchHints);
        this.edgeId = edgeId;
        this.edgeDirection = edgeDirection;
        this.edgeLabel = edgeLabel;
        this.otherVertexId = otherVertexId;
        this.edgeVisibility = edgeVisibility;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public Direction getEdgeDirection() {
        return edgeDirection;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public String getOtherVertexId() {
        return otherVertexId;
    }

    public Visibility getEdgeVisibility() {
        return edgeVisibility;
    }

    @Override
    public String toString() {
        return String.format(
            "%s, edgeId='%s', edgeDirection=%s, edgeLabel='%s', otherVertexId='%s', edgeVisibility=%s}",
            super.toString(),
            getEdgeId(),
            getEdgeDirection(),
            getEdgeLabel(),
            getOtherVertexId(),
            getEdgeVisibility()
        );
    }
}
