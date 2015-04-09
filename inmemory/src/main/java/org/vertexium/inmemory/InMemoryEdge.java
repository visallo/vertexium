package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.util.EnumSet;

public class InMemoryEdge extends InMemoryElement implements Edge {
    private final String outVertexId;
    private final String inVertexId;
    private String label;

    protected InMemoryEdge(
            Graph graph,
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Visibility visibility,
            Iterable<Property> properties,
            InMemoryHistoricalPropertyValues historicalPropertyValues,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            Authorizations authorizations
    ) {
        super(
                graph,
                edgeId,
                visibility,
                properties,
                historicalPropertyValues,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                authorizations
        );
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case IN:
                return inVertexId;
            case OUT:
                return outVertexId;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Vertex getVertex(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), fetchHints, authorizations);
    }

    @Override
    public Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getVertex(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public String getOtherVertexId(String myVertexId) {
        if (inVertexId.equals(myVertexId)) {
            return outVertexId;
        } else if (outVertexId.equals(myVertexId)) {
            return inVertexId;
        }
        throw new VertexiumException("myVertexId does not appear on either the in or the out.");
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        return getOtherVertex(myVertexId, FetchHint.ALL, authorizations);
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), fetchHints, authorizations);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingEdgeMutation prepareMutation() {
        return new ExistingEdgeMutation(this) {
            @Override
            public Edge save(Authorizations authorizations) {
                saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }

    void setLabel(String newEdgeLabel) {
        this.label = newEdgeLabel;
    }
}
