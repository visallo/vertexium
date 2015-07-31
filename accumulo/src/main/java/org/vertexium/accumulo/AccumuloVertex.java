package org.vertexium.accumulo;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
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
import org.vertexium.util.JoinIterable;
import org.vertexium.util.LookAheadIterable;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;

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
                new EdgesWithEdgeInfo(),
                new EdgesWithEdgeInfo(),
                timestamp,
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
            Edges inEdges,
            Edges outEdges,
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

    public static Vertex createFromIteratorValue(AccumuloGraph graph, Key key, Value value, Authorizations authorizations) {
        try {
            String vertexId;
            Visibility vertexVisibility;
            Iterable<Property> properties;
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = new ArrayList<>();
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = new ArrayList<>();
            Iterable<Visibility> hiddenVisibilities;
            Edges inEdges;
            Edges outEdges;
            long timestamp;

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.get());
            final DataInputStream in = new DataInputStream(byteArrayInputStream);
            DataInputStreamUtils.decodeHeader(in, ElementData.TYPE_ID_VERTEX);
            vertexId = DataInputStreamUtils.decodeText(in).toString();
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
            outEdges = DataInputStreamUtils.decodeEdges(in, graph.getNameSubstitutionStrategy());
            inEdges = DataInputStreamUtils.decodeEdges(in, graph.getNameSubstitutionStrategy());

            return new AccumuloVertex(
                    graph,
                    vertexId,
                    vertexVisibility,
                    properties,
                    propertyDeleteMutations,
                    propertySoftDeleteMutations,
                    hiddenVisibilities,
                    inEdges,
                    outEdges,
                    timestamp,
                    authorizations
            );
        } catch (IOException ex) {
            throw new VertexiumException("Could not read vertex", ex);
        }
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
        Set<String> edgeLabels = new HashSet<>();

        if (direction == Direction.IN || direction == Direction.BOTH) {
            if (inEdges instanceof EdgesWithCount) {
                edgeLabels.addAll(((EdgesWithCount) inEdges).getLabels());
            } else {
                edgeLabels.addAll(toList(new ConvertingIterable<Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo>, String>(getEdgeInfos(Direction.IN)) {
                    @Override
                    protected String convert(Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo> o) {
                        return o.getValue().getLabel();
                    }
                }));
            }
        }

        if (direction == Direction.OUT || direction == Direction.BOTH) {
            if (outEdges instanceof EdgesWithCount) {
                edgeLabels.addAll(((EdgesWithCount) outEdges).getLabels());
            } else {
                edgeLabels.addAll(toList(new ConvertingIterable<Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo>, String>(getEdgeInfos(Direction.OUT)) {
                    @Override
                    protected String convert(Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo> o) {
                        return o.getValue().getLabel();
                    }
                }));
            }
        }

        return edgeLabels;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return getVertices(direction, FetchHint.ALL, authorizations);
    }

    @SuppressWarnings("unused")
    public Iterable<String> getEdgeIdsWithOtherVertexId(final String otherVertexId, final Direction direction, final String[] labels, final Authorizations authorizations) {
        return new LookAheadIterable<Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo>, String>() {
            @Override
            protected boolean isIncluded(Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo> edgeInfo, String edgeId) {
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
            protected String convert(Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo> edgeInfo) {
                return edgeInfo.getKey();
            }

            @Override
            protected Iterator<Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo>> createIterator() {
                return getEdgeInfos(direction).iterator();
            }
        };
    }

    private Iterable<Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo>> getEdgeInfos(Direction direction) {
        switch (direction) {
            case IN:
                if (this.inEdges instanceof EdgesWithEdgeInfo) {
                    return ((EdgesWithEdgeInfo) this.inEdges).getEdgeInfos();
                }
                throw new VertexiumException("Cannot get edge info");
            case OUT:
                if (this.outEdges instanceof EdgesWithEdgeInfo) {
                    return ((EdgesWithEdgeInfo) this.outEdges).getEdgeInfos();
                }
                throw new VertexiumException("Cannot get edge info");
            case BOTH:
                return new JoinIterable<>(getEdgeInfos(Direction.IN), getEdgeInfos(Direction.OUT));
            default:
                throw new VertexiumException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Iterable<org.vertexium.EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        return new ConvertingIterable<Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo>, org.vertexium.EdgeInfo>(getEdgeInfos(direction)) {
            @Override
            protected org.vertexium.EdgeInfo convert(Map.Entry<String, org.vertexium.accumulo.iterator.model.EdgeInfo> o) {
                final org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = o.getValue();
                return new EdgeInfo() {
                    @Override
                    public String getLabel() {
                        return edgeInfo.getLabel();
                    }

                    @Override
                    public String getVertexId() {
                        return edgeInfo.getVertexId();
                    }
                };
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
                if (this.inEdges instanceof EdgesWithEdgeInfo) {
                    return new GetVertexIdsIterable(((EdgesWithEdgeInfo) this.inEdges).getEdges().values(), labels);
                }
                throw new VertexiumException("Cannot get vertex ids");
            case OUT:
                if (this.outEdges instanceof EdgesWithEdgeInfo) {
                    return new GetVertexIdsIterable(((EdgesWithEdgeInfo) this.outEdges).getEdges().values(), labels);
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
}
