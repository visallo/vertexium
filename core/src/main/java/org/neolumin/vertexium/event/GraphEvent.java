package org.neolumin.vertexium.event;

import org.neolumin.vertexium.Graph;

public abstract class GraphEvent {
    private final Graph graph;

    protected GraphEvent(Graph graph) {
        this.graph = graph;
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GraphEvent) {
            GraphEvent other = (GraphEvent) obj;
            return getGraph().equals(other.getGraph());
        }
        return false;
    }
}
