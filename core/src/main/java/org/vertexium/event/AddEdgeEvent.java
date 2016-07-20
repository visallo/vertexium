package org.vertexium.event;

import org.vertexium.Edge;
import org.vertexium.Graph;

public class AddEdgeEvent extends GraphEvent {
    private final Edge edge;

    public AddEdgeEvent(Graph graph, Edge edge) {
        super(graph);
        this.edge = edge;
    }

    public Edge getEdge() {
        return edge;
    }

    @Override
    public String toString() {
        return "AddEdgeEvent{edge=" + edge + '}';
    }

    @Override
    public int hashCode() {
        return getEdge().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AddEdgeEvent)) {
            return false;
        }

        AddEdgeEvent other = (AddEdgeEvent) obj;
        return getEdge().equals(other.getEdge());
    }
}
