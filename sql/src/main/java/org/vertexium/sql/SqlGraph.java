package org.vertexium.sql;

import com.google.common.collect.Iterables;
import org.vertexium.*;
import org.vertexium.inmemory.*;
import org.vertexium.mutation.SetPropertyMetadata;
import org.vertexium.sql.collections.SqlMap;
import org.vertexium.sql.collections.Storable;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.LookAheadIterable;

import java.util.*;

public class SqlGraph extends InMemoryGraph {
    private final GraphMetadataStore metadataStore;
    private final SqlMap<InMemoryTableElement<InMemoryVertex>> vertexMap;
    private final SqlMap<InMemoryTableElement<InMemoryEdge>> edgeMap;

    public SqlGraph(SqlGraphConfiguration configuration) {
        this(configuration,
                new SqlVertexTable(configuration.newVertexMap()), new SqlEdgeTable(configuration.newEdgeMap()));
    }

    public SqlGraph(SqlGraphConfiguration configuration, SqlVertexTable vertices, SqlEdgeTable edges) {
        super(configuration, vertices, edges);
        vertexMap = configuration.newVertexMap();
        edgeMap = configuration.newEdgeMap();
        metadataStore = new SqlGraphMetadataStore(configuration);
    }

    public static SqlGraph create(SqlGraphConfiguration config) {
        SqlGraph graph = new SqlGraph(config);
        graph.setup();
        return graph;
    }

    @SuppressWarnings("unused")
    public static SqlGraph create(Map<String, Object> config) {
        return create(new SqlGraphConfiguration(config));
    }

    @Override
    public Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Long endTime,
                            Authorizations authorizations) {
        validateAuthorizations(authorizations);

        InMemoryTableElement<InMemoryVertex> element = vertexMap.get(vertexId);
        if (element == null || !isIncludedInTimeSpan(element, fetchHints, endTime, authorizations)) {
            return null;
        } else {
            return element.createElement(this, fetchHints.contains(FetchHint.INCLUDE_HIDDEN), endTime, authorizations);
        }
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(final String vertexIdPrefix, final EnumSet<FetchHint> fetchHints,
                                                  final Long endTime, final Authorizations authorizations) {
        validateAuthorizations(authorizations);

        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        return new LookAheadIterable<InMemoryTableVertex, Vertex>() {
            @Override
            protected boolean isIncluded(InMemoryTableVertex element, Vertex vertex) {
                return vertex != null && SqlGraph.this.isIncluded(element, fetchHints, authorizations);
            }

            @Override
            protected Vertex convert(InMemoryTableVertex element) {
                return element.createElement(SqlGraph.this, includeHidden, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryTableVertex> createIterator() {
                Iterator<InMemoryTableElement<InMemoryVertex>> elements = vertexMap.query("id like ?",
                        vertexIdPrefix + "%");

                return new ConvertingIterable<InMemoryTableElement<InMemoryVertex>, InMemoryTableVertex>(elements) {
                    @Override
                    protected InMemoryTableVertex convert(InMemoryTableElement<InMemoryVertex> element) {
                        return ((SqlTableVertex) element).asInMemoryTableVertex();
                    }
                }.iterator();
            }
        };
    }

    @Override
    public Edge getEdge(String edgeId, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        InMemoryTableElement<InMemoryEdge> element = edgeMap.get(edgeId);
        if (element == null || !isIncluded(element, fetchHints, authorizations)) {
            return null;
        } else {
            return element.createElement(this, fetchHints.contains(FetchHint.INCLUDE_HIDDEN), endTime, authorizations);
        }
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, final EnumSet<FetchHint> fetchHints, final Long endTime,
                                   final Authorizations authorizations) {
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        return new LookAheadIterable<InMemoryTableEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryTableEdge element, Edge edge) {
                return edge != null && SqlGraph.this.isIncluded(element, fetchHints, authorizations);
            }

            @Override
            protected Edge convert(InMemoryTableEdge element) {
                return element.createElement(SqlGraph.this, includeHidden, endTime, authorizations);
            }

            @SuppressWarnings("unused")
            @Override
            protected Iterator<InMemoryTableEdge> createIterator() {
                StringBuilder idWhere = new StringBuilder();
                boolean first = true;
                for (String id : ids) {
                    if (first) {
                        idWhere.append("id = ?");
                        first = false;
                    } else {
                        idWhere.append(" or id = ?");
                    }
                }

                if (first) {
                    return Collections.emptyIterator();
                } else {
                    Iterator<InMemoryTableElement<InMemoryEdge>> elements = edgeMap.query(idWhere.toString(),
                            Iterables.toArray(ids, Object.class));

                    return new ConvertingIterable<InMemoryTableElement<InMemoryEdge>, InMemoryTableEdge>(elements) {
                        @Override
                        protected InMemoryTableEdge convert(InMemoryTableElement<InMemoryEdge> element) {
                            return ((SqlTableEdge) element).asInMemoryTableEdge();
                        }
                    }.iterator();
                }
            }
        };
    }

    @Override
    public Iterable<Edge> getEdgesFromVertex(final String vertexId, final EnumSet<FetchHint> fetchHints,
                                             final Long endTime, final Authorizations authorizations) {
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        return new LookAheadIterable<InMemoryTableEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryTableEdge element, Edge edge) {
                return edge != null && SqlGraph.this.isIncluded(element, fetchHints, authorizations);
            }

            @Override
            protected Edge convert(InMemoryTableEdge element) {
                return element.createElement(SqlGraph.this, includeHidden, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryTableEdge> createIterator() {
                Iterator<InMemoryTableElement<InMemoryEdge>> elements =
                        edgeMap.query("in_vertex_id = ? or out_vertex_id = ?", vertexId, vertexId);

                return new ConvertingIterable<InMemoryTableElement<InMemoryEdge>, InMemoryTableEdge>(elements) {
                    @Override
                    protected InMemoryTableEdge convert(InMemoryTableElement<InMemoryEdge> element) {
                        return ((SqlTableEdge) element).asInMemoryTableEdge();
                    }
                }.iterator();
            }
        };
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return metadataStore;
    }

    @Override
    protected void alterElementPropertyMetadata(InMemoryTableElement inMemoryTableElement,
                                             List<SetPropertyMetadata> setPropertyMetadatas,
                                             Authorizations authorizations) {
        super.alterElementPropertyMetadata(inMemoryTableElement, setPropertyMetadatas, authorizations);
        ((Storable) inMemoryTableElement).store();
    }
}
