package org.vertexium.event;

import org.vertexium.Edge;
import org.vertexium.Graph;

public class RemoveEdgeEvent extends GraphEvent {
    private final Edge edge;

    public RemoveEdgeEvent(Graph graph, Edge edge) {
        super(graph);
        this.edge = edge;
    }

    public Edge getEdge() {
        return edge;
    }

    @Override
    public String toString() {
        return "RemoveEdgeEvent{edge=" + edge + '}';
    }

    @Override
    public int hashCode() {
        return getEdge().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RemoveEdgeEvent)) {
            return false;
        }

        RemoveEdgeEvent other = (RemoveEdgeEvent) obj;
        return getEdge().equals(other.getEdge()) && super.equals(obj);
    }
}
