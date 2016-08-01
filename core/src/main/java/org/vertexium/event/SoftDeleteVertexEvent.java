package org.vertexium.event;

import org.vertexium.Graph;
import org.vertexium.Vertex;

public class SoftDeleteVertexEvent extends GraphEvent {
    private final Vertex vertex;

    public SoftDeleteVertexEvent(Graph graph, Vertex vertex) {
        super(graph);
        this.vertex = vertex;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{vertex=" + vertex + '}';
    }

    @Override
    public int hashCode() {
        return getVertex().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SoftDeleteVertexEvent)) {
            return false;
        }

        SoftDeleteVertexEvent other = (SoftDeleteVertexEvent) obj;
        return getVertex().equals(other.getVertex());
    }
}
