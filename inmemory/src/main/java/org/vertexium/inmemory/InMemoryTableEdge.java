package org.vertexium.inmemory;

import org.vertexium.Authorizations;
import org.vertexium.FetchHints;

public class InMemoryTableEdge extends InMemoryTableElement<InMemoryEdge> {
    public InMemoryTableEdge(String id) {
        super(id);
    }

    @Override
    public InMemoryEdge createElementInternal(InMemoryGraph graph, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return new InMemoryEdge(graph, getId(), this, fetchHints, endTime, authorizations);
    }
}
