package org.vertexium.inmemory;

import org.vertexium.Authorizations;

public class InMemoryTableVertex extends InMemoryTableElement<InMemoryVertex> {
    public InMemoryTableVertex(String id) {
        super(id);
    }

    @Override
    public InMemoryVertex createElementInternal(InMemoryGraph graph, boolean includeHidden, Long endTime, Authorizations authorizations) {
        return new InMemoryVertex(graph, getId(), this, includeHidden, endTime, authorizations);
    }
}
