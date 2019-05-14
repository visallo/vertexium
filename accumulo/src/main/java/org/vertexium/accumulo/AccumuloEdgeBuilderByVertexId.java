package org.vertexium.accumulo;

import org.vertexium.*;

public abstract class AccumuloEdgeBuilderByVertexId extends EdgeBuilderByVertexId implements EdgeBuilderWithKeyValuePairs {
    private final ElementMutationBuilder elementMutationBuilder;

    protected AccumuloEdgeBuilderByVertexId(
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Visibility visibility,
        ElementMutationBuilder elementMutationBuilder
    ) {
        super(edgeId, outVertexId, inVertexId, label, visibility);
        this.elementMutationBuilder = elementMutationBuilder;
    }

    @Override
    public Iterable<KeyValuePair> getEdgeTableKeyValuePairs() {
        AccumuloEdge edge = createEdge(new AccumuloAuthorizations().getUser());
        return getElementMutationBuilder().getEdgeTableKeyValuePairsEdge(edge);
    }

    protected abstract AccumuloEdge createEdge(User user);

    @Override
    public Iterable<KeyValuePair> getVertexTableKeyValuePairs() {
        AccumuloEdge edge = createEdge(new AccumuloAuthorizations().getUser());
        return getElementMutationBuilder().getVertexTableKeyValuePairsEdge(edge);
    }

    public ElementMutationBuilder getElementMutationBuilder() {
        return elementMutationBuilder;
    }
}
