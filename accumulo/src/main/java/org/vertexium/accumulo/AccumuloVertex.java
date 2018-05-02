package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.VertexIterator;
import org.vertexium.accumulo.iterator.model.Edges;
import org.vertexium.accumulo.iterator.model.EdgesWithCount;
import org.vertexium.accumulo.iterator.model.EdgesWithEdgeInfo;
import org.vertexium.accumulo.iterator.model.ElementData;
import org.vertexium.accumulo.util.DataInputStreamUtils;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.query.VertexQuery;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;
import org.vertexium.util.JoinIterable;
import org.vertexium.util.LookAheadIterable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class AccumuloVertex extends AccumuloElement implements Vertex {
    public static final Text CF_SIGNAL = VertexIterator.CF_SIGNAL;
    public static final Text CF_OUT_EDGE = VertexIterator.CF_OUT_EDGE;
    public static final Text CF_IN_EDGE = VertexIterator.CF_IN_EDGE;
    public static final Text CF_OUT_EDGE_SOFT_DELETE = VertexIterator.CF_OUT_EDGE_SOFT_DELETE;
    public static final Text CF_IN_EDGE_SOFT_DELETE = VertexIterator.CF_IN_EDGE_SOFT_DELETE;
    public static final Text CF_OUT_EDGE_HIDDEN = VertexIterator.CF_OUT_EDGE_HIDDEN;
    public static final Text CF_IN_EDGE_HIDDEN = VertexIterator.CF_IN_EDGE_HIDDEN;
    private final Edges inEdges;
    private final Edges outEdges;

    public AccumuloVertex(
            AccumuloGraph graph,
            String vertexId,
            Visibility vertexVisibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            ImmutableSet<String> extendedDataTableNames,
            long timestamp,
            FetchHints fetchHints,
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
                extendedDataTableNames,
                new EdgesWithEdgeInfo(),
                new EdgesWithEdgeInfo(),
                timestamp,
                fetchHints,
                authorizations
        );
    }

    public AccumuloVertex(
            AccumuloGraph graph,
            String vertexId,
            Visibility vertexVisibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            ImmutableSet<String> extendedDataTableNames,
            Edges inEdges,
            Edges outEdges,
            long timestamp,
            FetchHints fetchHints,
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
                extendedDataTableNames,
                timestamp,
                fetchHints,
                authorizations
        );
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    public static Vertex createFromIteratorValue(
            AccumuloGraph graph,
            Key key,
            Value value,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        try {
            String vertexId;
            Visibility vertexVisibility;
            Iterable<Property> properties;
            Set<Visibility> hiddenVisibilities;
            Edges inEdges;
            Edges outEdges;
            long timestamp;

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.get());
            DataInputStream in = new DataInputStream(byteArrayInputStream);
            DataInputStreamUtils.decodeHeader(in, ElementData.TYPE_ID_VERTEX);
            vertexId = DataInputStreamUtils.decodeString(in);
            timestamp = in.readLong();
            vertexVisibility = new Visibility(DataInputStreamUtils.decodeString(in));

            ImmutableSet<String> hiddenVisibilityStrings = DataInputStreamUtils.decodeStringSet(in);
            hiddenVisibilities = hiddenVisibilityStrings != null ?
                    hiddenVisibilityStrings.stream().map(Visibility::new).collect(Collectors.toSet()) :
                    null;

            List<MetadataEntry> metadataEntries = DataInputStreamUtils.decodeMetadataEntries(in);
            properties = DataInputStreamUtils.decodeProperties(graph, in, metadataEntries, fetchHints);

            ImmutableSet<String> extendedDataTableNames = DataInputStreamUtils.decodeStringSet(in);
            outEdges = DataInputStreamUtils.decodeEdges(in, graph.getNameSubstitutionStrategy());
            inEdges = DataInputStreamUtils.decodeEdges(in, graph.getNameSubstitutionStrategy());

            return new AccumuloVertex(
                    graph,
                    vertexId,
                    vertexVisibility,
                    properties,
                    null,
                    null,
                    hiddenVisibilities,
                    extendedDataTableNames,
                    inEdges,
                    outEdges,
                    timestamp,
                    fetchHints,
                    authorizations
            );
        } catch (IOException ex) {
            throw new VertexiumException("Could not read vertex", ex);
        }
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getEdges(direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(direction, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getGraph().getEdges(getEdgeIds(direction, authorizations), fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getEdgeIdsWithOtherVertexId(null, direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, label, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getGraph().getEdges(getEdgeIds(direction, labelToArrayOrNull(label), authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getEdgeIdsWithOtherVertexId(null, direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Direction direction, final String[] labels, final Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdges(otherVertex, direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdges(otherVertex, direction, label, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label), authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(otherVertex, direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Vertex otherVertex, final Direction direction, final String[] labels, final Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations);
    }

    @Override
    @Deprecated
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        return getEdgesSummary(authorizations).getCountOfEdges(direction);
    }

    @Override
    @Deprecated
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        return getEdgesSummary(authorizations).getEdgeLabels(direction);
    }

    @Override
    public EdgesSummary getEdgesSummary(Authorizations authorizations) {
        Map<String, Integer> outEdgeCountsByLabels = new HashMap<>();
        Map<String, Integer> inEdgeCountsByLabels = new HashMap<>();

        if (inEdges instanceof EdgesWithCount) {
            EdgesWithCount edgesWithCount = (EdgesWithCount) this.inEdges;
            inEdgeCountsByLabels.putAll(edgesWithCount.getEdgeCountsByLabelName());
        } else {
            for (Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo> entry : getEdgeInfos(Direction.IN)) {
                String label = entry.getValue().getLabel();
                Integer c = inEdgeCountsByLabels.getOrDefault(label, 0);
                inEdgeCountsByLabels.put(label, c + 1);
            }
        }

        if (outEdges instanceof EdgesWithCount) {
            EdgesWithCount edgesWithCount = (EdgesWithCount) this.outEdges;
            outEdgeCountsByLabels.putAll(edgesWithCount.getEdgeCountsByLabelName());
        } else {
            for (Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo> entry : getEdgeInfos(Direction.OUT)) {
                String label = entry.getValue().getLabel();
                Integer c = outEdgeCountsByLabels.getOrDefault(label, 0);
                outEdgeCountsByLabels.put(label, c + 1);
            }
        }

        return new EdgesSummary(outEdgeCountsByLabels, inEdgeCountsByLabels);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return getVertices(direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @SuppressWarnings("unused")
    public Iterable<String> getEdgeIdsWithOtherVertexId(
            String otherVertexId,
            Direction direction,
            String[] labels,
            Authorizations authorizations
    ) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new VertexiumException("getEdgeIdsWithOtherVertexId called without including any edge infos");
        }
        return new LookAheadIterable<Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo>, String>() {
            @Override
            protected boolean isIncluded(Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo> edgeInfo, String edgeId) {
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
            protected String convert(Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo> edgeInfo) {
                return edgeInfo.getKey().toString();
            }

            @Override
            protected Iterator<Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo>> createIterator() {
                return getEdgeInfos(direction).iterator();
            }
        };
    }

    private Iterable<Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo>> getEdgeInfos(Direction direction) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new VertexiumException("getEdgeInfos called without including any edge infos");
        }
        switch (direction) {
            case IN:
                if (!getFetchHints().isIncludeInEdgeRefs() && !getFetchHints().hasEdgeLabelsOfEdgeRefsToInclude()) {
                    return null;
                }
                if (this.inEdges instanceof EdgesWithEdgeInfo) {
                    return ((EdgesWithEdgeInfo) this.inEdges).getEntries();
                }
                throw new VertexiumException("Cannot get edge info");
            case OUT:
                if (!getFetchHints().isIncludeOutEdgeRefs() && !getFetchHints().hasEdgeLabelsOfEdgeRefsToInclude()) {
                    return null;
                }
                if (this.outEdges instanceof EdgesWithEdgeInfo) {
                    return ((EdgesWithEdgeInfo) this.outEdges).getEntries();
                }
                throw new VertexiumException("Cannot get edge info");
            case BOTH:
                return new JoinIterable<>(getEdgeInfos(Direction.IN), getEdgeInfos(Direction.OUT));
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        String[] labels = null;
        return getEdgeInfos(direction, labels, authorizations);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations) {
        return getEdgeInfos(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<org.vertexium.EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new VertexiumException("getEdgeInfos called without including any edge infos");
        }
        switch (direction) {
            case IN:
                return filterEdgeInfosByLabel(accumuloEdgeInfosToEdgeInfos(getEdgeInfos(direction), Direction.IN), labels);
            case OUT:
                return filterEdgeInfosByLabel(accumuloEdgeInfosToEdgeInfos(getEdgeInfos(direction), Direction.OUT), labels);
            case BOTH:
                return new JoinIterable<>(getEdgeInfos(Direction.IN, labels, authorizations), getEdgeInfos(Direction.OUT, labels, authorizations));
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
    }

    private Iterable<EdgeInfo> filterEdgeInfosByLabel(Iterable<org.vertexium.EdgeInfo> edgeInfos, String[] labels) {
        if (labels != null) {
            return new FilterIterable<EdgeInfo>(edgeInfos) {
                @Override
                protected boolean isIncluded(EdgeInfo o) {
                    for (String label : labels) {
                        if (o.getLabel().equals(label)) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        }
        return edgeInfos;
    }

    private Iterable<org.vertexium.EdgeInfo> accumuloEdgeInfosToEdgeInfos(Iterable<Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo>> edgeInfos, Direction direction) {
        return new ConvertingIterable<Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo>, org.vertexium.EdgeInfo>(edgeInfos) {
            @Override
            protected org.vertexium.EdgeInfo convert(Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo> o) {
                final String edgeId = o.getKey().toString();
                final org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = o.getValue();
                return new EdgeInfo() {
                    @Override
                    public String getEdgeId() {
                        return edgeId;
                    }

                    @Override
                    public String getLabel() {
                        return edgeInfo.getLabel();
                    }

                    @Override
                    public String getVertexId() {
                        return edgeInfo.getVertexId();
                    }

                    @Override
                    public Direction getDirection() {
                        return direction;
                    }
                };
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, label, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return getVertices(direction, labelToArrayOrNull(label), fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        return getVertices(direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, final Authorizations authorizations) {
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
                if (this.inEdges instanceof EdgesWithEdgeInfo) {
                    return new GetVertexIdsIterable(((EdgesWithEdgeInfo) this.inEdges).getEdgeInfos(), labels);
                }
                throw new VertexiumException("Cannot get vertex ids");
            case OUT:
                if (this.outEdges instanceof EdgesWithEdgeInfo) {
                    return new GetVertexIdsIterable(((EdgesWithEdgeInfo) this.outEdges).getEdgeInfos(), labels);
                }
                throw new VertexiumException("Cannot get vertex ids");
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
        if (this.outEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.outEdges).add(edge.getId(), new org.vertexium.accumulo.iterator.model.EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN)));
        } else {
            throw new VertexiumException("Cannot add edge");
        }
    }

    void removeOutEdge(Edge edge) {
        if (this.outEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.outEdges).remove(edge.getId());
        } else {
            throw new VertexiumException("Cannot remove out edge");
        }
    }

    void addInEdge(Edge edge) {
        if (this.inEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.inEdges).add(edge.getId(), new org.vertexium.accumulo.iterator.model.EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT)));
        } else {
            throw new VertexiumException("Cannot add edge");
        }
    }

    void removeInEdge(Edge edge) {
        if (this.inEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.inEdges).remove(edge.getId());
        } else {
            throw new VertexiumException("Cannot remove in edge");
        }
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

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, authorizations), getGraph().getDefaultFetchHints(), null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, authorizations), fetchHints, null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, authorizations), fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, label, authorizations), getGraph().getDefaultFetchHints(), null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, label, authorizations), fetchHints, null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, authorizations), getGraph().getDefaultFetchHints(), null, authorizations);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, authorizations), fetchHints, null, authorizations);
    }

    private Iterable<EdgeVertexPair> getEdgeVertexPairs(Iterable<EdgeInfo> edgeInfos, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return EdgeVertexPair.getEdgeVertexPairs(getGraph(), getId(), edgeInfos, fetchHints, endTime, authorizations);
    }
}
