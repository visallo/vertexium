package org.vertexium.inmemory;

import org.vertexium.Authorizations;
import org.vertexium.FetchHints;

public class InMemoryTableVertex extends InMemoryTableElement<InMemoryVertex> {
    public InMemoryTableVertex(String id) {
        super(id);
    }

    @Override
    public InMemoryVertex createElementInternal(InMemoryGraph graph, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return new InMemoryVertex(graph, getId(), this, fetchHints, endTime, authorizations);
    }
}
