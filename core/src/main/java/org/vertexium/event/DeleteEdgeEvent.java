package org.vertexium.event;

import org.vertexium.EdgeElementLocation;
import org.vertexium.Graph;

public class DeleteEdgeEvent extends GraphEvent {
    private final EdgeElementLocation edge;

    public DeleteEdgeEvent(Graph graph, EdgeElementLocation edge) {
        super(graph);
        this.edge = edge;
    }

    public EdgeElementLocation getEdge() {
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
