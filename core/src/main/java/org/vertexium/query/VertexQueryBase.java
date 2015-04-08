package org.vertexium.query;

import org.vertexium.*;
import org.vertexium.*;
import org.vertexium.util.FilterIterable;

import java.util.EnumSet;
import java.util.Map;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    protected VertexQueryBase(Graph graph, Vertex sourceVertex, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    public abstract Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    @Override
    public abstract Iterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    @Override
    public Iterable<Edge> edges(final Direction direction, EnumSet<FetchHint> fetchHints) {
        return new FilterIterable<Edge>(edges(fetchHints)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                switch (direction) {
                    case BOTH:
                        return true;
                    case IN:
                        return edge.getVertexId(Direction.IN).equals(sourceVertex.getId());
                    case OUT:
                        return edge.getVertexId(Direction.OUT).equals(sourceVertex.getId());
                    default:
                        throw new RuntimeException("Unexpected direction: " + direction);
                }
            }
        };
    }

    @Override
    public Iterable<Edge> edges(final Direction direction) {
        return edges(direction, FetchHint.ALL);
    }

    @Override
    public Iterable<Edge> edges(Direction direction, final String label, EnumSet<FetchHint> fetchHints) {
        return new FilterIterable<Edge>(edges(direction, fetchHints)) {
            @Override
            protected boolean isIncluded(Edge o) {
                return label.equals(o.getLabel());
            }
        };
    }

    @Override
    public Iterable<Edge> edges(Direction direction, final String label) {
        return edges(direction, label, FetchHint.ALL);
    }

    public Vertex getSourceVertex() {
        return sourceVertex;
    }
}
