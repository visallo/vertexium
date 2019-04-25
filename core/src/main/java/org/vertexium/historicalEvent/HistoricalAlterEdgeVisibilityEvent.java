package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalAlterEdgeVisibilityEvent extends HistoricalAlterElementVisibilityEvent {
    private final String outVertexId;
    private final String inVertexId;
    private final String edgeLabel;

    public HistoricalAlterEdgeVisibilityEvent(
        String elementId,
        String outVertexId,
        String inVertexId,
        String edgeLabel,
        Visibility oldVisibility,
        Visibility newVisibility,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.EDGE, elementId, oldVisibility, newVisibility, timestamp, data, fetchHints);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.edgeLabel = edgeLabel;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    @Override
    public String toString() {
        return String.format(
            "%s, outVertexId=%s, inVertexId=%s, edgeLabel=%s}",
            super.toString(),
            getOutVertexId(),
            getInVertexId(),
            getEdgeLabel()
        );
    }
}
