package org.vertexium.event;

import org.vertexium.Graph;
import org.vertexium.Vertex;

public class DeleteVertexEvent extends GraphEvent {
    private final Vertex vertex;

    public DeleteVertexEvent(Graph graph, Vertex vertex) {
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
        if (!(obj instanceof DeleteVertexEvent)) {
            return false;
        }

        DeleteVertexEvent other = (DeleteVertexEvent) obj;
        return getVertex().equals(other.getVertex());
    }
}
