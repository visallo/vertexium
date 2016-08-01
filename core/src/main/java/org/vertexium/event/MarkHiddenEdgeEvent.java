package org.vertexium.event;

import org.vertexium.Edge;
import org.vertexium.Graph;

public class MarkHiddenEdgeEvent extends GraphEvent {
    private final Edge edge;

    public MarkHiddenEdgeEvent(Graph graph, Edge edge) {
        super(graph);
        this.edge = edge;
    }

    public Edge getEdge() {
        return edge;
    }

    @Override
    public String toString() {
        return "MarkHiddenEdgeEvent{edge=" + edge + '}';
    }

    @Override
    public int hashCode() {
        return getEdge().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MarkHiddenEdgeEvent)) {
            return false;
        }

        MarkHiddenEdgeEvent other = (MarkHiddenEdgeEvent) obj;
        return getEdge().equals(other.getEdge());
    }
}
