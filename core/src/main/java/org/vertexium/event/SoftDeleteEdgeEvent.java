package org.vertexium.event;

import org.vertexium.Edge;
import org.vertexium.Graph;

public class SoftDeleteEdgeEvent extends GraphEvent {
    private final Edge edge;

    public SoftDeleteEdgeEvent(Graph graph, Edge edge) {
        super(graph);
        this.edge = edge;
    }

    public Edge getEdge() {
        return edge;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{edge=" + edge + '}';
    }

    @Override
    public int hashCode() {
        return getEdge().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SoftDeleteEdgeEvent)) {
            return false;
        }

        SoftDeleteEdgeEvent other = (SoftDeleteEdgeEvent) obj;
        return getEdge().equals(other.getEdge());
    }
}
