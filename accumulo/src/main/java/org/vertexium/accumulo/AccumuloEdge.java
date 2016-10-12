package org.vertexium.accumulo;

import com.google.common.base.Function;
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

    public static Edge createFromIteratorValue(AccumuloGraph graph, Key key, Value value, Authorizations authorizations) {
        try {
            String edgeId;
            Visibility vertexVisibility;
            Iterable<Property> properties;
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = new ArrayList<>();
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = new ArrayList<>();
            Iterable<Visibility> hiddenVisibilities;
            Map<String, org.vertexium.accumulo.iterator.model.EdgeInfo> inEdges;
            Map<String, org.vertexium.accumulo.iterator.model.EdgeInfo> outEdges;
            long timestamp;

            ByteArrayInputStream bain = new ByteArrayInputStream(value.get());
            final DataInputStream in = new DataInputStream(bain);
            DataInputStreamUtils.decodeHeader(in, ElementData.TYPE_ID_EDGE);
            edgeId = DataInputStreamUtils.decodeText(in).toString();
            timestamp = in.readLong();
            vertexVisibility = new Visibility(DataInputStreamUtils.decodeText(in).toString());
            hiddenVisibilities = Iterables.transform(DataInputStreamUtils.decodeTextList(in), new Function<Text, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(Text input) {
                    return new Visibility(input.toString());
                }
            });
            properties = DataInputStreamUtils.decodeProperties(graph, in);
            String inVertexId = DataInputStreamUtils.decodeText(in).toString();
            String outVertexId = DataInputStreamUtils.decodeText(in).toString();
            String label = graph.getNameSubstitutionStrategy().inflate(DataInputStreamUtils.decodeText(in));

            return new AccumuloEdge(
                    graph,
                    edgeId,
                    outVertexId,
                    inVertexId,
                    label,
                    null,
                    vertexVisibility,
                    properties,
                    propertyDeleteMutations,
                    propertySoftDeleteMutations,
                    hiddenVisibilities,
                    timestamp,
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
        return getVertex(direction, FetchHint.ALL, authorizations);
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
        return getOtherVertex(myVertexId, FetchHint.ALL, authorizations);
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), fetchHints, authorizations);
    }

    @Override
    public EdgeVertices getVertices(Authorizations authorizations) {
        List<String> ids = new ArrayList<>();
        ids.add(getVertexId(Direction.OUT));
        ids.add(getVertexId(Direction.IN));
        Map<String, Vertex> vertices = IterableUtils.toMapById(getGraph().getVertices(ids, authorizations));
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
