package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.inmemory.util.EdgeToEdgeIdIterable;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.query.VertexQuery;
import org.vertexium.search.IndexHint;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;

import java.util.HashMap;
import java.util.Map;

public class InMemoryVertex extends InMemoryElement<InMemoryVertex> implements Vertex {
    public InMemoryVertex(
            InMemoryGraph graph,
            String id,
            InMemoryTableVertex inMemoryTableElement,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        super(
                graph,
                id,
                inMemoryTableElement,
                fetchHints,
                endTime,
                authorizations
        );
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getEdges(direction, getGraph().getDefaultFetchHints(), authorizations);
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
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, final String[] labels, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        Iterable<EdgeInfo> results = internalGetEdgeInfo(direction, authorizations);
        results = new FilterIterable<EdgeInfo>(results) {
            @Override
            protected boolean isIncluded(EdgeInfo o) {
                if (!getFetchHints().isIncludeEdgeRefLabel(o.getLabel())) {
                    return false;
                }
                if (labels == null) {
                    return true;
                } else {
                    for (String label : labels) {
                        if (o.getLabel().equals(label)) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        };
        return results;
    }

    private Iterable<EdgeInfo> internalGetEdgeInfo(Direction direction, Authorizations authorizations) {
        return new ConvertingIterable<Edge, EdgeInfo>(internalGetEdges(direction, getFetchHints(), null, authorizations)) {
            @Override
            protected EdgeInfo convert(Edge edge) {
                return new EdgeInfo() {
                    @Override
                    public String getEdgeId() {
                        return edge.getId();
                    }

                    @Override
                    public String getLabel() {
                        return edge.getLabel();
                    }

                    @Override
                    public String getVertexId() {
                        return edge.getOtherVertexId(InMemoryVertex.this.getId());
                    }

                    @Override
                    public Direction getDirection() {
                        return edge.getVertexId(Direction.OUT).equals(this.getVertexId())
                                ? Direction.OUT
                                : Direction.IN;
                    }
                };
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(direction, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return internalGetEdges(direction, fetchHints, endTime, authorizations);
    }

    private Iterable<Edge> internalGetEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return new FilterIterable<Edge>(getGraph().getEdgesFromVertex(getId(), fetchHints, endTime, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                switch (direction) {
                    case IN:
                        return edge.getVertexId(Direction.IN).equals(getId());
                    case OUT:
                        return edge.getVertexId(Direction.OUT).equals(getId());
                    default:
                        return true;
                }
            }
        };
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, getFetchHints(), authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, label, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, label, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                if (labels == null) {
                    return true;
                }
                for (String label : labels) {
                    if (label.equals(edge.getLabel())) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, labels, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdges(otherVertex, direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdges(otherVertex, direction, label, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, label, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, label, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(otherVertex, direction, labels, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, labels, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, labels, authorizations));
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

        for (EdgeInfo entry : internalGetEdgeInfo(Direction.IN, authorizations)) {
            String label = entry.getLabel();
            Integer c = inEdgeCountsByLabels.getOrDefault(label, 0);
            inEdgeCountsByLabels.put(label, c + 1);
        }

        for (EdgeInfo entry : internalGetEdgeInfo(Direction.OUT, authorizations)) {
            String label = entry.getLabel();
            Integer c = outEdgeCountsByLabels.getOrDefault(label, 0);
            outEdgeCountsByLabels.put(label, c + 1);
        }

        return new EdgesSummary(outEdgeCountsByLabels, inEdgeCountsByLabels);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return getVertices(direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final FetchHints fetchHints, final Authorizations authorizations) {
        return getVertices(direction, (String[]) null, fetchHints, authorizations);
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
    public Iterable<Vertex> getVertices(final Direction direction, final String[] labels, final FetchHints fetchHints, final Authorizations authorizations) {
        Iterable<String> vertexIds = getVertexIds(direction, labels, authorizations);
        return getGraph().getVertices(vertexIds, fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return getVertexIds(direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        return new ConvertingIterable<EdgeInfo, String>(getEdgeInfos(direction, labels, authorizations)) {
            @Override
            protected String convert(EdgeInfo e) {
                return e.getVertexId();
            }
        };
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        return getVertexIds(direction, (String[]) null, authorizations);
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingElementMutation<Vertex> prepareMutation() {
        return new ExistingElementMutationImpl<Vertex>(this) {
            @Override
            public Vertex save(Authorizations authorizations) {
                IndexHint indexHint = getIndexHint();
                Visibility oldElementVisibility = InMemoryVertex.this.getVisibility();
                saveExistingElementMutation(this, indexHint, authorizations);
                Vertex vertex = getElement();
                if (indexHint != IndexHint.DO_NOT_INDEX) {
                    saveMutationToSearchIndex(
                            vertex,
                            oldElementVisibility,
                            getNewElementVisibility(),
                            getAlterPropertyVisibilities(),
                            getExtendedData(),
                            authorizations
                    );
                }
                return vertex;
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
