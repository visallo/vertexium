package org.vertexium.inmemory;

import org.vertexium.util.ConvertingIterable;

import java.util.Map;

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

    public Iterable<InMemoryTableEdge> getAllTableElements() {
        return new ConvertingIterable<InMemoryTableElement<InMemoryEdge>, InMemoryTableEdge>(super.getRowValues()) {
            @Override
            protected InMemoryTableEdge convert(InMemoryTableElement<InMemoryEdge> o) {
                return (InMemoryTableEdge) o;
            }
        };
    }
}
