package org.vertexium.event;

import org.vertexium.Graph;
import org.vertexium.Vertex;

public class SoftDeleteVertexEvent extends GraphEvent {
    private final Vertex vertex;
    private final Object data;

    public SoftDeleteVertexEvent(Graph graph, Vertex vertex, Object data) {
        super(graph);
        this.vertex = vertex;
        this.data = data;
    }

    public Vertex getVertex() {
        return vertex;
    }

    public Object getData() {
        return data;
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
