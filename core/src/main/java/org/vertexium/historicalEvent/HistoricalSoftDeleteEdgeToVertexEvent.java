package org.vertexium.historicalEvent;

import org.vertexium.Direction;
import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalSoftDeleteEdgeToVertexEvent extends HistoricalEvent {
    private final String edgeId;
    private final Direction edgeDirection;
    private final String edgeLabel;
    private final String otherVertexId;
    private final Visibility edgeVisibility;
    private final Object data;

    public HistoricalSoftDeleteEdgeToVertexEvent(
        String elementId,
        String edgeId,
        Direction edgeDirection,
        String edgeLabel,
        String otherVertexId,
        Visibility edgeVisibility,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.VERTEX, elementId, timestamp, fetchHints);
        this.edgeId = edgeId;
        this.edgeDirection = edgeDirection;
        this.edgeLabel = edgeLabel;
        this.otherVertexId = otherVertexId;
        this.edgeVisibility = edgeVisibility;
        this.data = data;
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

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format(
            "%s, edgeId='%s', edgeDirection=%s, edgeLabel=%s, otherVertexId=%s, edgeVisibility=%s, data=%s}",
            super.toString(),
            getEdgeId(),
            getEdgeDirection(),
            getEdgeLabel(),
            getOtherVertexId(),
            getEdgeVisibility(),
            getData()
        );
    }
}
