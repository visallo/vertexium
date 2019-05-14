package org.vertexium.accumulo;

import org.vertexium.*;

public abstract class AccumuloVertexBuilder extends VertexBuilder implements VertexBuilderWithKeyValuePairs {
    private final ElementMutationBuilder elementMutationBuilder;

    public AccumuloVertexBuilder(String vertexId, Visibility visibility, ElementMutationBuilder elementMutationBuilder) {
        super(vertexId, visibility);
        this.elementMutationBuilder = elementMutationBuilder;
    }

    @Override
    public Iterable<KeyValuePair> getKeyValuePairs() {
        AccumuloVertex vertex = createVertex(new AccumuloAuthorizations().getUser());
        return getElementMutationBuilder().getKeyValuePairsForVertex(vertex);
    }

    @Override
    public abstract Vertex save(Authorizations authorizations);

    protected abstract AccumuloVertex createVertex(User user);

    public ElementMutationBuilder getElementMutationBuilder() {
        return elementMutationBuilder;
    }
}
