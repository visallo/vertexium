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
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.query.VertexQuery;
import org.vertexium.util.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.toIterable;

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
        Iterable<String> additionalVisibilities,
        ImmutableSet<String> extendedDataTableNames,
        long timestamp,
        FetchHints fetchHints,
        User user
    ) {
        this(
            graph,
            vertexId,
            vertexVisibility,
            properties,
            propertyDeleteMutations,
            propertySoftDeleteMutations,
            hiddenVisibilities,
            additionalVisibilities,
            extendedDataTableNames,
            new EdgesWithEdgeInfo(),
            new EdgesWithEdgeInfo(),
            timestamp,
            fetchHints,
            user
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
        Iterable<String> additionalVisibilities,
        ImmutableSet<String> extendedDataTableNames,
        Edges inEdges,
        Edges outEdges,
        long timestamp,
        FetchHints fetchHints,
        User user
    ) {
        super(
            graph,
            vertexId,
            vertexVisibility,
            properties,
            propertyDeleteMutations,
            propertySoftDeleteMutations,
            hiddenVisibilities,
            additionalVisibilities,
            extendedDataTableNames,
            timestamp,
            fetchHints,
            user
        );
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    public static Vertex createFromIteratorValue(
        AccumuloGraph graph,
        Key key,
        Value value,
        FetchHints fetchHints,
        User user
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

            ImmutableSet<String> additionalVisibilities = DataInputStreamUtils.decodeStringSet(in);

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
                additionalVisibilities,
                extendedDataTableNames,
                inEdges,
                outEdges,
                timestamp,
                fetchHints,
                user
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
    public Stream<Edge> getEdges(Direction direction, User user) {
        return getEdges(direction, getGraph().getDefaultFetchHints(), user);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(direction, fetchHints, null, authorizations);
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, FetchHints fetchHints, User user) {
        return getEdges(direction, fetchHints, null, user);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getEdges(direction, fetchHints, endTime, authorizations.getUser()));
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getGraph().getEdges(toIterable(getEdgeIds(direction, user)), fetchHints, endTime, user);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return toIterable(getEdgeIdsWithOtherVertexId(null, direction, null));
    }

    @Override
    public Stream<String> getEdgeIds(Direction direction, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getEdgeIdsWithOtherVertexId(null, direction, null);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, label, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, String label, User user) {
        return getEdges(direction, label, getGraph().getDefaultFetchHints(), user);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(direction, label, fetchHints, authorizations.getUser()));
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getGraph().getEdges(toIterable(getEdgeIds(direction, labelToArrayOrNull(label), user)), fetchHints, user);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdgeIds(direction, label, authorizations.getUser()));
    }

    @Override
    public Stream<String> getEdgeIds(Direction direction, String label, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getEdgeIdsWithOtherVertexId(null, direction, labelToArrayOrNull(label));
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, String[] labels, User user) {
        return getEdges(direction, labels, getGraph().getDefaultFetchHints(), user);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(direction, labels, fetchHints, authorizations.getUser()));
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getGraph().getEdges(toIterable(getEdgeIdsWithOtherVertexId(null, direction, labels)), fetchHints, user);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeIds(direction, labels, authorizations.getUser()));
    }

    @Override
    public Stream<String> getEdgeIds(Direction direction, String[] labels, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getEdgeIdsWithOtherVertexId(null, direction, labels);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdges(otherVertex, direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, User user) {
        return getEdges(otherVertex, direction, getGraph().getDefaultFetchHints(), user);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, fetchHints, authorizations.getUser()));
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getGraph().getEdges(toIterable(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null)), fetchHints, user);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return toIterable(getEdgeIds(otherVertex, direction, authorizations.getUser()));
    }

    @Override
    public Stream<String> getEdgeIds(Vertex otherVertex, Direction direction, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdges(otherVertex, direction, label, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String label, User user) {
        return getEdges(otherVertex, direction, label, getGraph().getDefaultFetchHints(), user);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, label, fetchHints, authorizations.getUser()));
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getGraph().getEdges(toIterable(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label))), fetchHints, user);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdgeIds(otherVertex, direction, label, authorizations.getUser()));
    }

    @Override
    public Stream<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label));
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(otherVertex, direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, User user) {
        return getEdges(otherVertex, direction, labels, getGraph().getDefaultFetchHints(), user);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdges(otherVertex, direction, labels, fetchHints, authorizations.getUser()));
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getGraph().getEdges(toIterable(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels)), fetchHints, user);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeIds(otherVertex, direction, labels, authorizations.getUser()));
    }

    @Override
    public Stream<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels);
    }

    @Override
    public EdgesSummary getEdgesSummary(Authorizations authorizations) {
        return getEdgesSummary(authorizations.getUser());
    }

    @Override
    public EdgesSummary getEdgesSummary(User user) {
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

    @Override
    public Stream<Vertex> getVertices(Direction direction, User user) {
        return getVertices(direction, getGraph().getDefaultFetchHints(), user);
    }

    @SuppressWarnings("unused")
    public Stream<String> getEdgeIdsWithOtherVertexId(String otherVertexId, Direction direction, String[] labels) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new VertexiumException("getEdgeIdsWithOtherVertexId called without including any edge infos");
        }
        return StreamUtils.stream(getEdgeInfos(direction).iterator())
                .filter(entry -> {
                    if (otherVertexId != null) {
                        if (!otherVertexId.equals(entry.getValue().getVertexId())) {
                            return false;
                        }
                    }
                    if (labels == null || labels.length == 0) {
                        return true;
                    }

                    for (String label : labels) {
                        if (label.equals(entry.getValue().getLabel())) {
                            return true;
                        }
                    }
                    return false;
                }).map(entry -> entry.getKey().toString());
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
        return getEdgeInfos(direction, (String[]) null, authorizations);
    }

    @Override
    public Stream<EdgeInfo> getEdgeInfos(Direction direction, User user) {
        return getEdgeInfos(direction, (String[]) null, user);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations) {
        return getEdgeInfos(direction, new String[]{label}, authorizations);
    }

    @Override
    public Stream<EdgeInfo> getEdgeInfos(Direction direction, String label, User user) {
        return getEdgeInfos(direction, new String[]{label}, user);
    }

    @Override
    public Iterable<org.vertexium.EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeInfos(direction, labels, authorizations.getUser()));
    }

    @Override
    public Stream<org.vertexium.EdgeInfo> getEdgeInfos(Direction direction, String[] labels, User user) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new VertexiumException("getEdgeInfos called without including any edge infos");
        }
        switch (direction) {
            case IN:
                return filterEdgeInfosByLabel(accumuloEdgeInfosToEdgeInfos(getEdgeInfos(direction), Direction.IN), labels);
            case OUT:
                return filterEdgeInfosByLabel(accumuloEdgeInfosToEdgeInfos(getEdgeInfos(direction), Direction.OUT), labels);
            case BOTH:
                return Stream.concat(getEdgeInfos(Direction.IN, labels, user), getEdgeInfos(Direction.OUT, labels, user));
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
    }

    private Stream<EdgeInfo> filterEdgeInfosByLabel(Stream<org.vertexium.EdgeInfo> edgeInfos, String[] labels) {
        if (labels != null) {
            return edgeInfos.filter(edgeInfo -> {
                for (String label : labels) {
                    if (edgeInfo.getLabel().equals(label)) {
                        return true;
                    }
                }
                return false;
            });
        }
        return edgeInfos;
    }

    private Stream<org.vertexium.EdgeInfo> accumuloEdgeInfosToEdgeInfos(Iterable<Map.Entry<Text, org.vertexium.accumulo.iterator.model.EdgeInfo>> edgeInfos, Direction direction) {
        return StreamUtils.stream(edgeInfos)
                .map(edgeInfoEntry -> {
                    final String edgeId = edgeInfoEntry.getKey().toString();
                    final org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = edgeInfoEntry.getValue();
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
                });
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, authorizations), fetchHints, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, FetchHints fetchHints, User user) {
        return getGraph().getVertices(toIterable(getVertexIds(direction, user)), fetchHints, user);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, label, getGraph().getDefaultFetchHints(), null, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String label, User user) {
        return getVertices(direction, label, getGraph().getDefaultFetchHints(), null, user);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Long endTime, Authorizations authorizations) {
        return getVertices(direction, label, getGraph().getDefaultFetchHints(), endTime, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String label, Long endTime, User user) {
        return getVertices(direction, label, getGraph().getDefaultFetchHints(), endTime, user);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return getVertices(direction, labelToArrayOrNull(label), fetchHints, null, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, User user) {
        return getVertices(direction, labelToArrayOrNull(label), fetchHints, null, user);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return getVertices(direction, labelToArrayOrNull(label), fetchHints, endTime, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(direction, labelToArrayOrNull(label), fetchHints, endTime, user);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        return getVertices(direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String[] labels, User user) {
        return getVertices(direction, labels, getGraph().getDefaultFetchHints(), user);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return getVertices(direction, labels, fetchHints, null, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, User user) {
        return getVertices(direction, labels, fetchHints, null, user);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, labels, authorizations), fetchHints, endTime, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
        return getGraph().getVertices(toIterable(getVertexIds(direction, labels, user)), fetchHints, endTime, user);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return getVertexIds(direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Stream<String> getVertexIds(Direction direction, String label, User user) {
        return getVertexIds(direction, labelToArrayOrNull(label), user);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        return getVertexIds(direction, (String[]) null, authorizations);
    }

    @Override
    public Stream<String> getVertexIds(Direction direction, User user) {
        return getVertexIds(direction, (String[]) null, user);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getVertexIds(direction, labels, authorizations.getUser()));
    }

    @Override
    public Stream<String> getVertexIds(Direction direction, String[] labels, User user) {
        Stream<org.vertexium.accumulo.iterator.model.EdgeInfo> edgeInfos;
        switch (direction) {
            case BOTH:
                Stream<String> inVertexIds = getVertexIds(Direction.IN, labels, user);
                Stream<String> outVertexIds = getVertexIds(Direction.OUT, labels, user);
                return Stream.concat(inVertexIds, outVertexIds);
            case IN:
                if (!(this.inEdges instanceof EdgesWithEdgeInfo)) {
                    throw new VertexiumException("Cannot get vertex ids");
                }
                edgeInfos = ((EdgesWithEdgeInfo) this.inEdges).getEdgeInfos();
                break;
            case OUT:
                if (!(this.outEdges instanceof EdgesWithEdgeInfo)) {
                    throw new VertexiumException("Cannot get vertex ids");
                }
                edgeInfos = ((EdgesWithEdgeInfo) this.outEdges).getEdgeInfos();
                break;
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
        return edgeInfos.filter(edgeInfo -> {
            if (labels == null || labels.length == 0) {
                return true;
            }
            for (String label : labels) {
                if (edgeInfo.getLabel().equals(label)) {
                    return true;
                }
            }
            return false;
        }).map(org.vertexium.accumulo.iterator.model.EdgeInfo::getVertexId);
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
            public String save(User user) {
                return saveVertex(user).getId();
            }

            @Override
            public Vertex save(Authorizations authorizations) {
                return saveVertex(authorizations.getUser());
            }

            private Vertex saveVertex(User user) {
                saveExistingElementMutation(this, user);
                return getElement();
            }
        };
    }

    private static String[] labelToArrayOrNull(String label) {
        return label == null ? null : new String[]{label};
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(getEdgeInfos(direction, authorizations.getUser()), getGraph().getDefaultFetchHints(), null, authorizations.getUser()));
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, user), getGraph().getDefaultFetchHints(), null, user);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(getEdgeInfos(direction, authorizations.getUser()), fetchHints, null, authorizations.getUser()));
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, user), fetchHints, null, user);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(getEdgeInfos(direction, authorizations.getUser()), fetchHints, endTime, authorizations.getUser()));
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, user), fetchHints, endTime, user);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(getEdgeInfos(direction, label, authorizations.getUser()), getGraph().getDefaultFetchHints(), null, authorizations.getUser()));
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, label, user), getGraph().getDefaultFetchHints(), null, user);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(getEdgeInfos(direction, label, authorizations.getUser()), fetchHints, null, authorizations.getUser()));
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, label, user), fetchHints, null, user);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(getEdgeInfos(direction, labels, authorizations.getUser()), getGraph().getDefaultFetchHints(), null, authorizations.getUser()));
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, user), getGraph().getDefaultFetchHints(), null, user);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return toIterable(getEdgeVertexPairs(getEdgeInfos(direction, labels, authorizations.getUser()), fetchHints, null, authorizations.getUser()));
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, user), fetchHints, null, user);
    }
    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, user), fetchHints, endTime, user);
    }

    private Stream<EdgeVertexPair> getEdgeVertexPairs(Stream<EdgeInfo> edgeInfos, FetchHints fetchHints, Long endTime, User user) {
        return EdgeVertexPair.getEdgeVertexPairs(getGraph(), getId(), edgeInfos, fetchHints, endTime, user);
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }
}
