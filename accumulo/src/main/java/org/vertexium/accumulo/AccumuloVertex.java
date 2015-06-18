package org.vertexium.accumulo;

import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.query.VertexQuery;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.JoinIterable;
import org.vertexium.util.LookAheadIterable;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toSet;

public class AccumuloVertex extends AccumuloElement implements Vertex {
    public static final String CF_SIGNAL_STRING = "V";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    public static final String CF_OUT_EDGE_STRING = "EOUT";
    public static final Text CF_OUT_EDGE = new Text(CF_OUT_EDGE_STRING);
    public static final String CF_OUT_EDGE_HIDDEN_STRING = "EOUTH";
    public static final Text CF_OUT_EDGE_HIDDEN = new Text(CF_OUT_EDGE_HIDDEN_STRING);
    public static final String CF_OUT_EDGE_SOFT_DELETE_STRING = "EOUTD";
    public static final Text CF_OUT_EDGE_SOFT_DELETE = new Text(CF_OUT_EDGE_SOFT_DELETE_STRING);
    public static final String CF_IN_EDGE_STRING = "EIN";
    public static final Text CF_IN_EDGE = new Text(CF_IN_EDGE_STRING);
    public static final String CF_IN_EDGE_HIDDEN_STRING = "EINH";
    public static final Text CF_IN_EDGE_HIDDEN = new Text(CF_IN_EDGE_HIDDEN_STRING);
    public static final String CF_IN_EDGE_SOFT_DELETE_STRING = "EIND";
    public static final Text CF_IN_EDGE_SOFT_DELETE = new Text(CF_IN_EDGE_SOFT_DELETE_STRING);
    private final Map<String, EdgeInfo> inEdges;
    private final Map<String, EdgeInfo> outEdges;

    public AccumuloVertex(
            AccumuloGraph graph,
            String vertexId,
            Visibility vertexVisibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            long timestamp,
            Authorizations authorizations
    ) {
        this(
                graph,
                vertexId,
                vertexVisibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                new HashMap<String, EdgeInfo>(),
                new HashMap<String, EdgeInfo>(),
                timestamp,
                authorizations
        );
    }

    AccumuloVertex(
            AccumuloGraph graph,
            String vertexId,
            Visibility vertexVisibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            Map<String, EdgeInfo> inEdges,
            Map<String, EdgeInfo> outEdges,
            long timestamp,
            Authorizations authorizations
    ) {
        super(
                graph,
                vertexId,
                vertexVisibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                timestamp,
                authorizations
        );
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getEdges(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdges(direction, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIds(direction, authorizations), fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIds(direction, labelToArrayOrNull(label), authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Direction direction, final String[] labels, final Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdges(otherVertex, direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdges(otherVertex, direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label), authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(otherVertex, direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Vertex otherVertex, final Direction direction, final String[] labels, final Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations);
    }

    @Override
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        return count(getEdgeIds(direction, authorizations));
    }

    @Override
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        return toSet(new ConvertingIterable<Map.Entry<String, EdgeInfo>, String>(getEdgeInfos(direction)) {
            @Override
            protected String convert(Map.Entry<String, EdgeInfo> o) {
                return o.getValue().getLabel();
            }
        });
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return getVertices(direction, FetchHint.ALL, authorizations);
    }

    @SuppressWarnings("unused")
    public Iterable<String> getEdgeIdsWithOtherVertexId(final String otherVertexId, final Direction direction, final String[] labels, final Authorizations authorizations) {
        return new LookAheadIterable<Map.Entry<String, EdgeInfo>, String>() {
            @Override
            protected boolean isIncluded(Map.Entry<String, EdgeInfo> edgeInfo, String edgeId) {
                if (otherVertexId != null) {
                    if (!otherVertexId.equals(edgeInfo.getValue().getVertexId())) {
                        return false;
                    }
                }
                if (labels == null || labels.length == 0) {
                    return true;
                }

                for (String label : labels) {
                    if (label.equals(edgeInfo.getValue().getLabel())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected String convert(Map.Entry<String, EdgeInfo> edgeInfo) {
                return edgeInfo.getKey();
            }

            @Override
            protected Iterator<Map.Entry<String, EdgeInfo>> createIterator() {
                return getEdgeInfos(direction).iterator();
            }
        };
    }

    private Iterable<Map.Entry<String, EdgeInfo>> getEdgeInfos(Direction direction) {
        switch (direction) {
            case IN:
                return this.inEdges.entrySet();
            case OUT:
                return this.outEdges.entrySet();
            case BOTH:
                return new JoinIterable<>(this.inEdges.entrySet(), this.outEdges.entrySet());
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Iterable<org.vertexium.EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        return new ConvertingIterable<Map.Entry<String, EdgeInfo>, org.vertexium.EdgeInfo>(getEdgeInfos(direction)) {
            @Override
            protected org.vertexium.EdgeInfo convert(Map.Entry<String, EdgeInfo> o) {
                return o.getValue();
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getVertices(direction, labelToArrayOrNull(label), fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        return getVertices(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return getVertexIds(direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        return getVertexIds(direction, (String[]) null, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                Iterable<String> inVertexIds = getVertexIds(Direction.IN, labels, authorizations);
                Iterable<String> outVertexIds = getVertexIds(Direction.OUT, labels, authorizations);
                return new JoinIterable<>(inVertexIds, outVertexIds);
            case IN:
                return new GetVertexIdsIterable(this.inEdges.values(), labels);
            case OUT:
                return new GetVertexIdsIterable(this.outEdges.values(), labels);
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    void addOutEdge(Edge edge) {
        this.outEdges.put(edge.getId(), new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN), getGraph().getNameSubstitutionStrategy()));
    }

    void removeOutEdge(Edge edge) {
        this.outEdges.remove(edge.getId());
    }

    void addInEdge(Edge edge) {
        this.inEdges.put(edge.getId(), new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT), getGraph().getNameSubstitutionStrategy()));
    }

    void removeInEdge(Edge edge) {
        this.inEdges.remove(edge.getId());
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingElementMutation<Vertex> prepareMutation() {
        return new ExistingElementMutationImpl<Vertex>(this) {
            @Override
            public Vertex save(Authorizations authorizations) {
                saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }

    private static String[] labelToArrayOrNull(String label) {
        return label == null ? null : new String[]{label};
    }
}
