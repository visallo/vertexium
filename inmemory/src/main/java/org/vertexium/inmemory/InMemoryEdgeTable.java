package org.vertexium.inmemory;

import java.util.stream.Stream;

public class InMemoryEdgeTable extends InMemoryTable<InMemoryEdge> {
    @Override
    protected InMemoryTableElement<InMemoryEdge> createInMemoryTableElement(String id) {
        return new InMemoryTableEdge(id);
    }

    public Stream<InMemoryTableEdge> getAllTableElements() {
        return super.getRowValues().map(r -> (InMemoryTableEdge) r);
    }
}
