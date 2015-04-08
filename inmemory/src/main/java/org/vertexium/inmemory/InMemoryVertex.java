package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.inmemory.util.EdgeToEdgeIdIterable;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationImpl;
import org.vertexium.mutation.PropertyRemoveMutation;
import org.vertexium.query.VertexQuery;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;
import org.vertexium.util.IterableUtils;
import org.vertexium.*;

import java.util.EnumSet;

public class InMemoryVertex extends InMemoryElement implements Vertex {
    public InMemoryVertex(
            Graph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            InMemoryHistoricalPropertyValues historicalPropertyValues,
            Iterable<PropertyRemoveMutation> propertyRemoveMutations,
            Iterable<Visibility> hiddenVisibilities,
            Authorizations authorizations
    ) {
        super(graph, id, visibility, properties, historicalPropertyValues, propertyRemoveMutations, hiddenVisibilities, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getEdges(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        return new ConvertingIterable<Edge, EdgeInfo>(getEdges(direction, authorizations)) {
            @Override
            protected EdgeInfo convert(final Edge o) {
                return new EdgeInfo() {
                    @Override
                    public String getLabel() {
                        return o.getLabel();
                    }

                    @Override
                    public String getVertexId() {
                        return o.getOtherVertexId(InMemoryVertex.this.getId());
                    }
                };
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return new FilterIterable<Edge>(getGraph().getEdgesFromVertex(getId(), fetchHints, authorizations)) {
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
        return new EdgeToEdgeIdIterable(getEdges(direction, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdges(direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, label, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
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
        return getEdges(otherVertex, direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
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
        return getEdges(otherVertex, direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
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
        return getEdges(otherVertex, direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
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
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        return IterableUtils.count(getEdgeIds(direction, authorizations));
    }

    @Override
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        return IterableUtils.toSet(new ConvertingIterable<Edge, String>(getEdges(direction, authorizations)) {
            @Override
            protected String convert(Edge o) {
                return o.getLabel();
            }
        });
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return getVertices(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        return new ConvertingIterable<Edge, Vertex>(getEdges(direction, authorizations)) {
            @Override
            protected Vertex convert(Edge edge) {
                return getOtherVertexFromEdge(edge, authorizations);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getVertices(direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        return getVertices(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(final Direction direction, final String[] labels, final EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        return new ConvertingIterable<Edge, Vertex>(getEdges(direction, labels, authorizations)) {
            @Override
            protected Vertex convert(Edge edge) {
                return getOtherVertexFromEdge(edge, authorizations);
            }
        };
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return new ConvertingIterable<Vertex, String>(getVertices(direction, label, authorizations)) {
            @Override
            protected String convert(Vertex o) {
                return o.getId();
            }
        };
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        return new ConvertingIterable<Vertex, String>(getVertices(direction, labels, authorizations)) {
            @Override
            protected String convert(Vertex o) {
                return o.getId();
            }
        };
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        return new ConvertingIterable<Vertex, String>(getVertices(direction, authorizations)) {
            @Override
            protected String convert(Vertex o) {
                return o.getId();
            }
        };
    }

    private Vertex getOtherVertexFromEdge(Edge edge, Authorizations authorizations) {
        if (edge.getVertexId(Direction.IN).equals(getId())) {
            return edge.getVertex(Direction.OUT, authorizations);
        }
        if (edge.getVertexId(Direction.OUT).equals(getId())) {
            return edge.getVertex(Direction.IN, authorizations);
        }
        throw new IllegalStateException("Edge does not contain vertex on either end");
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
                saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }

    @SuppressWarnings("unused")
    public static InMemoryVertex updateOrCreate(InMemoryGraph graph, InMemoryVertex existingVertex, InMemoryVertex newVertex, Authorizations authorizations) {
        if (existingVertex == null) {
            return newVertex;
        }

        existingVertex.updateExisting(newVertex);
        return existingVertex;
    }
}
