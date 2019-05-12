package org.vertexium.inmemory;

public class InMemoryVertexTable extends InMemoryTable<InMemoryVertex> {
    public InMemoryVertexTable() {
        super();
    }

    @Override
    protected InMemoryTableElement<InMemoryVertex> createInMemoryTableElement(String id) {
        return new InMemoryTableVertex(id);
    }
}
