package org.vertexium.event;

import org.vertexium.Graph;

import java.util.HashMap;

public class FlushEvent extends GraphEvent {

    private final int HASHCODE = 52937;
    public FlushEvent(Graph graph) {
        super(graph);

    }

    @Override
    public int hashCode() {
        return HASHCODE;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FlushEvent;
    }
}
