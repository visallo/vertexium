package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.query.VertexQuery;
import org.vertexium.search.IndexHint;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

    @Override
    public Stream<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return internalGetEdgeInfo(direction, user)
            .filter(o -> {
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
            });
    }

    private Stream<EdgeInfo> internalGetEdgeInfo(Direction direction, User user) {
        return internalGetEdges(direction, getFetchHints(), null, user)
            .map(edge -> new EdgeInfo() {
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
                        ? Direction.IN
                        : Direction.OUT;
                }
            });
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return internalGetEdges(direction, fetchHints, endTime, user);
    }

    private Stream<Edge> internalGetEdges(Direction direction, FetchHints fetchHints, Long endTime, User user) {
        return getGraph().getEdgesFromVertex(getId(), fetchHints, endTime, user)
            .filter(edge -> {
                switch (direction) {
                    case IN:
                        return edge.getVertexId(Direction.IN).equals(getId());
                    case OUT:
                        return edge.getVertexId(Direction.OUT).equals(getId());
                    default:
                        return true;
                }
            });
    }

    @Override
    public Stream<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, User user) {
        return getEdges(direction, user)
            .filter(edge -> {
                if (labels == null) {
                    return true;
                }
                for (String label : labels) {
                    if (label.equals(edge.getLabel())) {
                        return true;
                    }
                }
                return false;
            });
    }

    @Override
    public Stream<String> getEdgeIds(Direction direction, String[] labels, User user) {
        return getEdges(direction, labels, FetchHints.NONE, user)
            .map(Element::getId);
    }

    @Override
    public Stream<Edge> getEdges(final Vertex otherVertex, Direction direction, FetchHints fetchHints, User user) {
        return getEdges(direction, fetchHints, user)
            .filter(edge -> edge.getOtherVertexId(getId()).equals(otherVertex.getId()));
    }

    @Override
    public Stream<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, User user) {
        return getEdges(direction, labels, user)
            .filter(edge -> edge.getOtherVertexId(getId()).equals(otherVertex.getId()));
    }

    @Override
    public Stream<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, User user) {
        return getEdges(otherVertex, direction, labels, user)
            .map(Element::getId);
    }

    @Override
    public EdgesSummary getEdgesSummary(User user) {
        Map<String, Integer> outEdgeCountsByLabels = new HashMap<>();
        Map<String, Integer> inEdgeCountsByLabels = new HashMap<>();

        internalGetEdgeInfo(Direction.IN, user).forEach(entry -> {
            String label = entry.getLabel();
            Integer c = inEdgeCountsByLabels.getOrDefault(label, 0);
            inEdgeCountsByLabels.put(label, c + 1);
        });

        internalGetEdgeInfo(Direction.OUT, user).forEach(entry -> {
            String label = entry.getLabel();
            Integer c = outEdgeCountsByLabels.getOrDefault(label, 0);
            outEdgeCountsByLabels.put(label, c + 1);
        });

        return new EdgesSummary(outEdgeCountsByLabels, inEdgeCountsByLabels);
    }

    @Override
    public Stream<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
        Iterable<String> vertexIds = getVertexIds(direction, labels, user).collect(Collectors.toList());
        return getGraph().getVertices(vertexIds, fetchHints, endTime, user);
    }

    @Override
    public Stream<String> getVertexIds(Direction direction, String[] labels, User user) {
        return getEdgeInfos(direction, labels, user)
            .map(EdgeInfo::getVertexId);
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
                User user = authorizations.getUser();
                return saveVertex(user);
            }

            @Override
            public String save(User user) {
                return saveVertex(user).getId();
            }

            private Vertex saveVertex(User user) {
                IndexHint indexHint = getIndexHint();
                saveExistingElementMutation(this, indexHint, user);
                Vertex vertex = getElement();
                if (indexHint != IndexHint.DO_NOT_INDEX) {
                    getGraph().updateElementAndExtendedDataInSearchIndex(vertex, this, user);
                }
                return vertex;
            }
        };
    }

    @Override
    public Stream<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
        return getEdgeVertexPairs(getEdgeInfos(direction, labels, user), fetchHints, endTime, user);
    }

    private Stream<EdgeVertexPair> getEdgeVertexPairs(Stream<EdgeInfo> edgeInfos, FetchHints fetchHints, Long endTime, User user) {
        return EdgeVertexPair.getEdgeVertexPairs(getGraph(), getId(), edgeInfos, fetchHints, endTime, user);
    }
}
