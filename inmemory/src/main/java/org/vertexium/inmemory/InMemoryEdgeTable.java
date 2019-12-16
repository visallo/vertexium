package org.vertexium.inmemory;

import org.vertexium.MetadataPlugin;
import org.vertexium.util.ConvertingIterable;

import java.util.Map;

public class InMemoryEdgeTable extends InMemoryTable<InMemoryEdge> {
    public InMemoryEdgeTable(Map<String, InMemoryTableElement<InMemoryEdge>> rows, MetadataPlugin metadataPlugin) {
        super(rows, metadataPlugin);
    }

    public InMemoryEdgeTable(MetadataPlugin metadataPlugin) {
        super(metadataPlugin);
    }

    @Override
    protected InMemoryTableElement<InMemoryEdge> createInMemoryTableElement(String id, MetadataPlugin metadataPlugin) {
        return new InMemoryTableEdge(id, metadataPlugin);
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
