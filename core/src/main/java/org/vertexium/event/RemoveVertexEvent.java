package org.vertexium.event;

import org.vertexium.Graph;
import org.vertexium.Vertex;

public class RemoveVertexEvent extends GraphEvent {
    private final Vertex vertex;

    public RemoveVertexEvent(Graph graph, Vertex vertex) {
        super(graph);
        this.vertex = vertex;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return "RemoveVertexEvent{vertex=" + vertex + '}';
    }

    @Override
    public int hashCode() {
        return getVertex().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RemoveVertexEvent)) {
            return false;
        }

        RemoveVertexEvent other = (RemoveVertexEvent) obj;
        return getVertex().equals(other.getVertex()) && super.equals(obj);
    }
}
