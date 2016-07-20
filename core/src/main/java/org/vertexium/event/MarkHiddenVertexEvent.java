package org.vertexium.event;

import org.vertexium.Graph;
import org.vertexium.Vertex;

public class MarkHiddenVertexEvent extends GraphEvent {
    private final Vertex vertex;

    public MarkHiddenVertexEvent(Graph graph, Vertex vertex) {
        super(graph);
        this.vertex = vertex;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return "MarkHiddenVertexEvent{vertex=" + vertex + '}';
    }

    @Override
    public int hashCode() {
        return getVertex().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MarkHiddenVertexEvent)) {
            return false;
        }

        MarkHiddenVertexEvent other = (MarkHiddenVertexEvent) obj;
        return getVertex().equals(other.getVertex());
    }
}
