package org.vertexium.inmemory;

public class InMemoryVertexTable extends InMemoryTable<InMemoryVertex> {
    @Override
    protected InMemoryTableElement<InMemoryVertex> createInMemoryTableElement(String id) {
        return new InMemoryTableVertex(id);
    }
}
