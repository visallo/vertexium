package org.vertexium.event;

import org.vertexium.Graph;

public abstract class GraphEvent {
    private final Graph graph;

    protected GraphEvent(Graph graph) {
        this.graph = graph;
    }

    public Graph getGraph() {
        return graph;
    }
}
