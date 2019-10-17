package org.vertexium.accumulo;

import org.vertexium.Authorizations;
import org.vertexium.Edge;
import org.vertexium.EdgeBuilderByVertexId;
import org.vertexium.Visibility;

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
    public abstract Edge save(Authorizations authorizations);

    @Override
    public Iterable<KeyValuePair> getEdgeTableKeyValuePairs() {
        AccumuloEdge edge = createEdge(new AccumuloAuthorizations());
        return getElementMutationBuilder().getEdgeTableKeyValuePairsEdge(edge);
    }

    protected abstract AccumuloEdge createEdge(Authorizations authorizations);

    @Override
    public Iterable<KeyValuePair> getVertexTableKeyValuePairs() {
        AccumuloEdge edge = createEdge(new AccumuloAuthorizations());
        return getElementMutationBuilder().getVertexTableKeyValuePairsEdge(edge);
    }

    public ElementMutationBuilder getElementMutationBuilder() {
        return elementMutationBuilder;
    }
}
