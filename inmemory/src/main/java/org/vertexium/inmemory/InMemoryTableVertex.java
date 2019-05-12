package org.vertexium.inmemory;

import org.vertexium.ElementType;
import org.vertexium.FetchHints;
import org.vertexium.User;

public class InMemoryTableVertex extends InMemoryTableElement<InMemoryVertex> {
    private static final long serialVersionUID = -7587878000189582069L;

    public InMemoryTableVertex(String id) {
        super(id);
    }

    @Override
    protected ElementType getElementType() {
        return ElementType.VERTEX;
    }

    @Override
    public InMemoryVertex createElementInternal(InMemoryGraph graph, FetchHints fetchHints, Long endTime, User user) {
        return new InMemoryVertex(graph, getId(), this, fetchHints, endTime, user);
    }
}
