package org.vertexium.inmemory;

import org.vertexium.Authorizations;
import org.vertexium.ElementType;
import org.vertexium.FetchHints;
import org.vertexium.MetadataPlugin;

public class InMemoryTableVertex extends InMemoryTableElement<InMemoryVertex> {
    public InMemoryTableVertex(String id, MetadataPlugin metadataPlugin) {
        super(id, metadataPlugin);
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

    @Override
    public InMemoryVertex createElementInternal(InMemoryGraph graph, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return new InMemoryVertex(graph, getId(), this, fetchHints, endTime, authorizations);
    }
}
