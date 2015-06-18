package org.vertexium.accumulo;

import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.util.EnumSet;

public class AccumuloEdge extends AccumuloElement implements Edge {
    public static final String CF_SIGNAL_STRING = "E";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    public static final String CF_OUT_VERTEX_STRING = "EOUT";
    public static final Text CF_OUT_VERTEX = new Text(CF_OUT_VERTEX_STRING);
    public static final String CF_IN_VERTEX_STRING = "EIN";
    public static final Text CF_IN_VERTEX = new Text(CF_IN_VERTEX_STRING);
    private final String outVertexId;
    private final String inVertexId;
    private final String label;
    private final String newEdgeLabel;

    public AccumuloEdge(
            Graph graph,
            String id,
            String outVertexId,
            String inVertexId,
            String label,
            String newEdgeLabel,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            long timestamp,
            Authorizations authorizations
    ) {
        super(
                graph,
                id,
                visibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                timestamp,
                authorizations
        );
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
        this.newEdgeLabel = newEdgeLabel;
    }

    String getNewEdgeLabel() {
        return newEdgeLabel;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return outVertexId;
            case IN:
                return inVertexId;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
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
        throw new VertexiumException("myVertexId does not appear on either the in (" + inVertexId + ") or the out (" + outVertexId + ").");
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
    public Vertex getVertex(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), fetchHints, authorizations);
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
}
