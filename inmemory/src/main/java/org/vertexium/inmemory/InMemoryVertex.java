package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationBase;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class InMemoryVertex extends InMemoryElement<InMemoryVertex> implements Vertex {
    public InMemoryVertex(
        InMemoryGraph graph,
        String id,
        InMemoryTableVertex inMemoryTableElement,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        super(
            graph,
            id,
            inMemoryTableElement,
            fetchHints,
            endTime,
            user
        );
    }

    @Override
    public Stream<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Long endTime, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return internalGetEdgeInfo(direction, endTime, getFetchHints(), user)
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

    private Stream<EdgeInfo> internalGetEdgeInfo(Direction direction, Long endTime, FetchHints fetchHints, User user) {
        return internalGetEdges(direction, null, fetchHints, endTime, user)
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

                @Override
                public Visibility getVisibility() {
                    return edge.getVisibility();
                }
            });
    }

    private Stream<Edge> internalGetEdges(Direction direction, String[] labels, FetchHints fetchHints, Long endTime, User user) {
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
            })
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
    public Stream<Edge> getEdges(
        Vertex otherVertex,
        Direction direction,
        String[] labels,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        Stream<Edge> edges = internalGetEdges(direction, labels, fetchHints, endTime, user);
        if (otherVertex == null) {
            return edges;
        }
        return edges.filter(edge -> edge.getOtherVertexId(getId()).equals(otherVertex.getId()));
    }

    @Override
    public EdgesSummary getEdgesSummary(User user) {
        Map<String, Integer> outEdgeCountsByLabels = new HashMap<>();
        Map<String, Integer> inEdgeCountsByLabels = new HashMap<>();

        FetchHints fetchHints = getFetchHints();

        internalGetEdgeInfo(Direction.IN, null, fetchHints, user).forEach(entry -> {
            String label = entry.getLabel();
            Integer c = inEdgeCountsByLabels.getOrDefault(label, 0);
            inEdgeCountsByLabels.put(label, c + 1);
        });

        internalGetEdgeInfo(Direction.OUT, null, fetchHints, user).forEach(entry -> {
            String label = entry.getLabel();
            Integer c = outEdgeCountsByLabels.getOrDefault(label, 0);
            outEdgeCountsByLabels.put(label, c + 1);
        });

        return new EdgesSummary(outEdgeCountsByLabels, inEdgeCountsByLabels);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingElementMutation<Vertex> prepareMutation() {
        return new ExistingElementMutationBase<Vertex>(this) {
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
                getGraph().getElementMutationBuilder().saveExistingElementMutation(this, user);
                return getElement();
            }
        };
    }
}
