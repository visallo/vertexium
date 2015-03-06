package org.neolumin.vertexium.event;

import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.Vertex;

public class AddVertexEvent extends GraphEvent {
    private final Vertex vertex;

    public AddVertexEvent(Graph graph, Vertex vertex) {
        super(graph);
        this.vertex = vertex;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return "AddVertexEvent{vertex=" + vertex + '}';
    }

    @Override
    public int hashCode() {
        return this.vertex.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AddVertexEvent)) {
            return false;
        }

        AddVertexEvent other = (AddVertexEvent) obj;
        return getVertex().equals(other.getVertex()) && super.equals(obj);
    }
}
