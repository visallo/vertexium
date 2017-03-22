package org.vertexium.inmemory;

import org.vertexium.Authorizations;
import org.vertexium.FetchHint;

import java.util.EnumSet;

public class InMemoryTableEdge extends InMemoryTableElement<InMemoryEdge> {
    public InMemoryTableEdge(String id) {
        super(id);
    }

    @Override
    public InMemoryEdge createElementInternal(InMemoryGraph graph, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return new InMemoryEdge(graph, getId(), this, fetchHints, endTime, authorizations);
    }
}
