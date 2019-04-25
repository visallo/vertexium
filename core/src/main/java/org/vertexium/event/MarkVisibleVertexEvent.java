package org.vertexium.event;

import org.vertexium.Graph;
import org.vertexium.Vertex;

public class MarkVisibleVertexEvent extends GraphEvent {
    private final Vertex vertex;
    private final Object data;

    public MarkVisibleVertexEvent(Graph graph, Vertex vertex, Object data) {
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
        return "MarkVisibleVertexEvent{vertex=" + vertex + '}';
    }

    @Override
    public int hashCode() {
        return getVertex().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MarkVisibleVertexEvent)) {
            return false;
        }

        MarkVisibleVertexEvent other = (MarkVisibleVertexEvent) obj;
        return getVertex().equals(other.getVertex());
    }
}
