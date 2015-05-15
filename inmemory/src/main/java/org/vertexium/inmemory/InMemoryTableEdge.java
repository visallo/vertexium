package org.vertexium.inmemory;

import org.vertexium.Authorizations;

public class InMemoryTableEdge extends InMemoryTableElement<InMemoryEdge> {
    public InMemoryTableEdge(String id) {
        super(id);
    }

    @Override
    public InMemoryEdge createElementInternal(InMemoryGraph graph, boolean includeHidden, Long endTime, Authorizations authorizations) {
        return new InMemoryEdge(graph, getId(), this, includeHidden, endTime, authorizations);
    }
}
