package org.vertexium.accumulo;

import org.vertexium.Authorizations;
import org.vertexium.VertexBuilder;
import org.vertexium.Visibility;

public abstract class AccumuloVertexBuilder extends VertexBuilder implements VertexBuilderWithKeyValuePairs {
    private final ElementMutationBuilder elementMutationBuilder;

    public AccumuloVertexBuilder(String vertexId, Visibility visibility, ElementMutationBuilder elementMutationBuilder) {
        super(vertexId, visibility);
        this.elementMutationBuilder = elementMutationBuilder;
    }

    @Override
    public Iterable<KeyValuePair> getKeyValuePairs() {
        AccumuloVertex vertex = createVertex(new AccumuloAuthorizations());
        return getElementMutationBuilder().getKeyValuePairsForVertex(vertex);
    }

    protected abstract AccumuloVertex createVertex(Authorizations authorizations);

    public ElementMutationBuilder getElementMutationBuilder() {
        return elementMutationBuilder;
    }
}
