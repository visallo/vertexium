package org.vertexium.query;

import org.vertexium.Edge;
import org.vertexium.FetchHint;
import org.vertexium.Vertex;
import org.vertexium.util.SelectManyIterable;

import java.util.*;

public class CompositeGraphQuery implements Query {
    private final List<Query> queries;

    public CompositeGraphQuery(Query... queries) {
        this(Arrays.asList(queries));
    }

    public CompositeGraphQuery(Collection<Query> queries) {
        this.queries = new ArrayList<>(queries);
    }

    @Override
    public Iterable<Vertex> vertices() {
        return vertices(FetchHint.ALL);
    }

    @Override
    public Iterable<Vertex> vertices(final EnumSet<FetchHint> fetchHints) {
        final Set<String> seenIds = new HashSet<>();
        return new SelectManyIterable<Query, Vertex>(this.queries) {
            @Override
            public Iterable<Vertex> getIterable(Query query) {
                return query.vertices(fetchHints);
            }

            @Override
            protected boolean isIncluded(Vertex vertex) {
                if (seenIds.contains(vertex.getId())) {
                    return false;
                }
                seenIds.add(vertex.getId());
                return super.isIncluded(vertex);
            }
        };
    }

    @Override
    public Iterable<Edge> edges() {
        return edges(FetchHint.ALL);
    }

    @Override
    public Iterable<Edge> edges(final EnumSet<FetchHint> fetchHints) {
        final Set<String> seenIds = new HashSet<>();
        return new SelectManyIterable<Query, Edge>(this.queries) {
            @Override
            public Iterable<Edge> getIterable(Query query) {
                return query.edges(fetchHints);
            }

            @Override
            protected boolean isIncluded(Edge edge) {
                if (seenIds.contains(edge.getId())) {
                    return false;
                }
                seenIds.add(edge.getId());
                return super.isIncluded(edge);
            }
        };
    }

    @Override
    public Iterable<Edge> edges(final String label) {
        return edges(label, FetchHint.ALL);
    }

    @Override
    public Iterable<Edge> edges(final String label, final EnumSet<FetchHint> fetchHints) {
        final Set<String> seenIds = new HashSet<>();
        return new SelectManyIterable<Query, Edge>(this.queries) {
            @Override
            public Iterable<Edge> getIterable(Query query) {
                return query.edges(label, fetchHints);
            }

            @Override
            protected boolean isIncluded(Edge edge) {
                if (seenIds.contains(edge.getId())) {
                    return false;
                }
                seenIds.add(edge.getId());
                return super.isIncluded(edge);
            }
        };
    }

    @Override
    public <T> Query range(String propertyName, T startValue, T endValue) {
        for (Query query : queries) {
            query.range(propertyName, startValue, endValue);
        }
        return this;
    }

    @Override
    public <T> Query has(String propertyName, T value) {
        for (Query query : queries) {
            query.has(propertyName, value);
        }
        return this;
    }

    @Override
    public <T> Query has(String propertyName, Predicate predicate, T value) {
        for (Query query : queries) {
            query.has(propertyName, predicate, value);
        }
        return this;
    }

    @Override
    public Query skip(int count) {
        for (Query query : queries) {
            query.skip(count);
        }
        return this;
    }

    @Override
    public Query limit(int count) {
        for (Query query : queries) {
            query.limit(count);
        }
        return this;
    }
}
