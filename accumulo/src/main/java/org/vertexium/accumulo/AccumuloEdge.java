package org.vertexium.accumulo;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.EdgeIterator;
import org.vertexium.accumulo.iterator.model.ElementData;
import org.vertexium.accumulo.util.DataInputStreamUtils;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.util.IterableUtils;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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
            ImmutableSet<String> extendedDataTableNames,
            long timestamp,
            EnumSet<FetchHint> fetchHints,
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
            EnumSet<FetchHint> fetchHints,
            Authorizations authorizations
    ) {
        try {
            String edgeId;
            Visibility vertexVisibility;
            Iterable<Property> properties;
            Iterable<Visibility> hiddenVisibilities;
            long timestamp;

            ByteArrayInputStream bain = new ByteArrayInputStream(value.get());
            final DataInputStream in = new DataInputStream(bain);
            DataInputStreamUtils.decodeHeader(in, ElementData.TYPE_ID_EDGE);
            edgeId = DataInputStreamUtils.decodeString(in);
            timestamp = in.readLong();
            vertexVisibility = new Visibility(DataInputStreamUtils.decodeString(in));
            hiddenVisibilities = Iterables.transform(DataInputStreamUtils.decodeStringSet(in), new Function<String, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(String input) {
                    return new Visibility(input);
                }
            });
            properties = DataInputStreamUtils.decodeProperties(graph, in, fetchHints);
            ImmutableSet<String> extendedDataTableNames = DataInputStreamUtils.decodeStringSet(in);
            String inVertexId = DataInputStreamUtils.decodeString(in);
            String outVertexId = DataInputStreamUtils.decodeString(in);
            String label = graph.getNameSubstitutionStrategy().inflate(DataInputStreamUtils.decodeString(in));

            return new AccumuloEdge(
                    graph,
                    edgeId,
                    outVertexId,
                    inVertexId,
                    label,
                    null,
                    vertexVisibility,
                    properties,
                    null,
                    null,
                    hiddenVisibilities,
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
    public Vertex getOtherVertex(String myVertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), fetchHints, authorizations);
    }

    @Override
    public EdgeVertices getVertices(Authorizations authorizations) {
        return getVertices(getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public EdgeVertices getVertices(EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        List<String> ids = new ArrayList<>();
        ids.add(getVertexId(Direction.OUT));
        ids.add(getVertexId(Direction.IN));
        Map<String, Vertex> vertices = IterableUtils.toMapById(getGraph().getVertices(ids, fetchHints, authorizations));
        return new EdgeVertices(
                vertices.get(getVertexId(Direction.OUT)),
                vertices.get(getVertexId(Direction.IN))
        );
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
