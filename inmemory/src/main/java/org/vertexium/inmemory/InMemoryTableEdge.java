package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.historicalEvent.HistoricalAddEdgeToVertexEvent;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalSoftDeleteEdgeToVertexEvent;
import org.vertexium.inmemory.mutations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class InMemoryTableEdge extends InMemoryTableElement<InMemoryEdge> {
    private static final long serialVersionUID = 7193421350589782382L;

    public InMemoryTableEdge(String id) {
        super(id);
    }

    @Override
    protected ElementType getElementType() {
        return ElementType.EDGE;
    }

    @Override
    public InMemoryEdge createElementInternal(InMemoryGraph graph, FetchHints fetchHints, Long endTime, User user) {
        return new InMemoryEdge(graph, getId(), this, fetchHints, endTime, user);
    }

    public Stream<HistoricalEvent> getHistoricalEventsForVertex(
        String vertexId,
        HistoricalEventsFetchHints historicalEventsFetchHints
    ) {
        List<HistoricalEvent> results = new ArrayList<>();
        String otherVertexId = null;
        Direction direction = null;
        String edgeLabel = null;
        Visibility edgeVisibility = null;
        for (Mutation m : mutations) {
            boolean tryEmitAddEdgeToVertexEvent = false;
            if (m instanceof AlterVisibilityMutation) {
                edgeVisibility = m.getVisibility();
                tryEmitAddEdgeToVertexEvent = true;
            } else if (m instanceof AlterEdgeLabelMutation) {
                AlterEdgeLabelMutation alterEdgeLabelMutation = (AlterEdgeLabelMutation) m;
                edgeLabel = alterEdgeLabelMutation.getNewEdgeLabel();
                tryEmitAddEdgeToVertexEvent = true;
            } else if (m instanceof EdgeSetupMutation) {
                EdgeSetupMutation edgeSetupMutation = (EdgeSetupMutation) m;
                otherVertexId = edgeSetupMutation.getInVertexId().equals(vertexId)
                    ? edgeSetupMutation.getOutVertexId()
                    : edgeSetupMutation.getInVertexId();
                direction = edgeSetupMutation.getInVertexId().equals(vertexId)
                    ? Direction.IN
                    : Direction.OUT;
                tryEmitAddEdgeToVertexEvent = true;
            } else if (m instanceof SoftDeleteMutation) {
                SoftDeleteMutation softDeleteMutation = (SoftDeleteMutation) m;
                results.add(new HistoricalSoftDeleteEdgeToVertexEvent(
                    vertexId,
                    getId(),
                    direction,
                    edgeLabel,
                    otherVertexId,
                    edgeVisibility,
                    HistoricalEvent.zonedDateTimeFromTimestamp(m.getTimestamp()),
                    softDeleteMutation.getData(),
                    historicalEventsFetchHints
                ));
            }

            if (tryEmitAddEdgeToVertexEvent) {
                if (direction != null && edgeLabel != null && otherVertexId != null && edgeVisibility != null) {
                    results.add(new HistoricalAddEdgeToVertexEvent(
                        vertexId,
                        getId(),
                        direction,
                        edgeLabel,
                        otherVertexId,
                        edgeVisibility,
                        HistoricalEvent.zonedDateTimeFromTimestamp(m.getTimestamp()),
                        historicalEventsFetchHints
                    ));
                }
            }
        }
        return results.stream();
    }
}
