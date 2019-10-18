package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.EdgeIterator;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.util.StreamUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class AccumuloEdge extends AccumuloElement implements Edge {
    public static final Text CF_SIGNAL = EdgeIterator.CF_SIGNAL;
    public static final Text CF_OUT_VERTEX = EdgeIterator.CF_OUT_VERTEX;
    public static final Text CF_IN_VERTEX = EdgeIterator.CF_IN_VERTEX;
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
        Iterable<String> additionalVisibilities,
        ImmutableSet<String> extendedDataTableNames,
        long timestamp,
        FetchHints fetchHints,
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
            additionalVisibilities,
            extendedDataTableNames,
            timestamp,
            fetchHints,
            authorizations
        );
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
        this.newEdgeLabel = newEdgeLabel;
    }

    public static Edge createFromIteratorValue(
        AccumuloGraph graph,
        Key key,
        Value value,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        try {
            org.vertexium.accumulo.iterator.model.proto.Edge protoEdge =
                org.vertexium.accumulo.iterator.model.proto.Edge.parseFrom(value.get());
            String edgeId = protoEdge.getElement().getId().toStringUtf8();
            long timestamp = protoEdge.getElement().getTimestamp();
            Visibility edgeVisibility = new Visibility(protoEdge.getElement().getVisibility().toStringUtf8());
            Iterable<Visibility> hiddenVisibilities = protoEdge.getElement().getHiddenVisibilitiesList().stream()
                .map(hiddenVisibility -> new Visibility(hiddenVisibility.toStringUtf8()))
                .collect(Collectors.toSet());
            ImmutableSet<String> additionalVisibilities = protoEdge.getElement().getAdditionalVisibilitiesList().stream()
                .map(ByteString::toStringUtf8)
                .collect(StreamUtils.toImmutableSet());
            List<MetadataEntry> metadataEntries = createMetadataEntryFromIteratorValue(protoEdge.getElement().getMetadataEntriesList());
            Iterable<Property> properties = createPropertiesFromIteratorValue(graph, protoEdge.getElement().getPropertiesList(), metadataEntries, fetchHints);
            ImmutableSet<String> extendedDataTableNames = protoEdge.getElement().getExtendedTableNamesList().stream()
                .collect(StreamUtils.toImmutableSet());
            String inVertexId = protoEdge.getInVertexId().toStringUtf8();
            String outVertexId = protoEdge.getOutVertexId().toStringUtf8();
            String label = graph.getNameSubstitutionStrategy().inflate(protoEdge.getLabel().toStringUtf8());

            return new AccumuloEdge(
                graph,
                edgeId,
                outVertexId,
                inVertexId,
                label,
                null,
                edgeVisibility,
                properties,
                null,
                null,
                hiddenVisibilities,
                additionalVisibilities,
                extendedDataTableNames,
                timestamp,
                fetchHints,
                authorizations
            );
        } catch (IOException ex) {
            throw new VertexiumException("Could not read vertex", ex);
        }
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
        return getVertex(direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public String getOtherVertexId(String myVertexId) {
        if (inVertexId.equals(myVertexId)) {
            return outVertexId;
        } else if (outVertexId.equals(myVertexId)) {
            return inVertexId;
        }
        throw new VertexiumException("myVertexId(" + myVertexId + ") does not appear on edge (" + getId() + ") in either the in (" + inVertexId + ") or the out (" + outVertexId + ").");
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        return getOtherVertex(myVertexId, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, FetchHints fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), fetchHints, authorizations);
    }

    @Override
    public EdgeVertices getVertices(Authorizations authorizations) {
        return getVertices(getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Vertex getVertex(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), fetchHints, authorizations);
    }

    @Override
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
