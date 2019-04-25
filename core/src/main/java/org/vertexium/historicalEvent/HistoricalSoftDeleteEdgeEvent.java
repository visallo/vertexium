package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;

import java.time.ZonedDateTime;

public class HistoricalSoftDeleteEdgeEvent extends HistoricalSoftDeleteElementEvent {
    private final String outVertexId;
    private final String inVertexId;
    private final String edgeLabel;

    public HistoricalSoftDeleteEdgeEvent(
        String id,
        String outVertexId,
        String inVertexId,
        String edgeLabel,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(ElementType.EDGE, id, timestamp, data, fetchHints);
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
            "%s, outVertexId='%s', inVertexId='%s', edgeLabel='%s', data=%s}",
            super.toString(),
            getOutVertexId(),
            getInVertexId(),
            getEdgeLabel(),
            getData()
        );
    }
}
