package org.vertexium.event;

import org.vertexium.Edge;
import org.vertexium.Graph;

public class DeleteEdgeEvent extends GraphEvent {
    private final Edge edge;

    public DeleteEdgeEvent(Graph graph, Edge edge) {
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
        if (!(obj instanceof DeleteEdgeEvent)) {
            return false;
        }

        DeleteEdgeEvent other = (DeleteEdgeEvent) obj;
        return getEdge().equals(other.getEdge());
    }
}
