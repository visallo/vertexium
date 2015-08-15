package org.vertexium.inmemory;

import java.util.Map;

public class InMemoryVertexTable extends InMemoryTable<InMemoryVertex> {
    public InMemoryVertexTable(Map<String, InMemoryTableElement<InMemoryVertex>> rows) {
        super(rows);
    }

    public InMemoryVertexTable() {
        super();
    }

    @Override
    protected InMemoryTableElement<InMemoryVertex> createInMemoryTableElement(String id) {
        return new InMemoryTableVertex(id);
    }
}
