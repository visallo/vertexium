package org.vertexium.inmemory;

import org.vertexium.MetadataPlugin;

import java.util.Map;

public class InMemoryVertexTable extends InMemoryTable<InMemoryVertex> {
    public InMemoryVertexTable(Map<String, InMemoryTableElement<InMemoryVertex>> rows, MetadataPlugin metadataPlugin) {
        super(rows, metadataPlugin);
    }

    public InMemoryVertexTable(MetadataPlugin metadataPlugin) {
        super(metadataPlugin);
    }

    @Override
    protected InMemoryTableElement<InMemoryVertex> createInMemoryTableElement(String id, MetadataPlugin metadataPlugin) {
        return new InMemoryTableVertex(id, metadataPlugin);
    }
}
