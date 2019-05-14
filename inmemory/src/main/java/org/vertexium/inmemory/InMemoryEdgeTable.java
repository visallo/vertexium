package org.vertexium.inmemory;

import java.util.Map;
import java.util.stream.Stream;

public class InMemoryEdgeTable extends InMemoryTable<InMemoryEdge> {
    public InMemoryEdgeTable(Map<String, InMemoryTableElement<InMemoryEdge>> rows) {
        super(rows);
    }

    public InMemoryEdgeTable() {
    }

    @Override
    protected InMemoryTableElement<InMemoryEdge> createInMemoryTableElement(String id) {
        return new InMemoryTableEdge(id);
    }

    public Stream<InMemoryTableEdge> getAllTableElements() {
        return super.getRowValues().map(r -> (InMemoryTableEdge) r);
    }
}
