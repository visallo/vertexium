package org.vertexium.sql;

import com.google.common.collect.Iterables;
import org.vertexium.*;
import org.vertexium.inmemory.*;
import org.vertexium.mutation.SetPropertyMetadata;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.sql.collections.SqlMap;
import org.vertexium.sql.collections.Storable;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.LookAheadIterable;

import java.util.*;

public class SqlGraph extends InMemoryGraph {
    private final SqlMap<InMemoryTableElement<InMemoryVertex>> vertexMap;
    private final SqlMap<InMemoryTableElement<InMemoryEdge>> edgeMap;
    private final SqlStreamingPropertyTable streamingPropertyTable;

    private static class ConfigHolder {
        final SqlGraphConfiguration configuration;
        final SqlMap<InMemoryTableElement<InMemoryVertex>> vertexMap;
        final SqlMap<InMemoryTableElement<InMemoryEdge>> edgeMap;
        final SqlExtendedDataTable extendedDataTable;
        final SqlStreamingPropertyTable streamingPropertyTable;

        ConfigHolder(SqlGraphConfiguration configuration) {
            this.configuration = configuration;
            this.vertexMap = configuration.newVertexMap();
            this.edgeMap = configuration.newEdgeMap();
            this.extendedDataTable = configuration.newExtendedDataTable();
            this.streamingPropertyTable = configuration.newStreamingPropertyTable();
        }
    }

    public SqlGraph(SqlGraphConfiguration configuration) {
        this(new ConfigHolder(configuration));
    }

    private SqlGraph(ConfigHolder configHolder) {
        super(
                configHolder.configuration,
                new SqlVertexTable(configHolder.vertexMap),
                new SqlEdgeTable(configHolder.edgeMap),
                configHolder.extendedDataTable
        );
        this.vertexMap = configHolder.vertexMap;
        this.edgeMap = configHolder.edgeMap;
        this.streamingPropertyTable = configHolder.streamingPropertyTable;
        this.vertexMap.setStorableContext(this);
        this.edgeMap.setStorableContext(this);
    }

    @Override
    protected GraphMetadataStore newGraphMetadataStore(GraphConfiguration configuration) {
        return new SqlGraphMetadataStore(((SqlGraphConfiguration) configuration).newMetadataMap());
    }

    public static SqlGraph create(SqlGraphConfiguration config) {
        if (config.isCreateTables()) {
            SqlGraphDDL.create(config.getDataSource(), config);
        }
        SqlGraph graph = new SqlGraph(config);
        graph.setup();
        return graph;
    }

    @SuppressWarnings("unused")
    public static SqlGraph create(Map<String, Object> config) {
        return create(new SqlGraphConfiguration(config));
    }

    @Override
    public Vertex getVertex(
            String vertexId, FetchHints fetchHints, Long endTime,
            Authorizations authorizations
    ) {
        validateAuthorizations(authorizations);

        InMemoryTableElement<InMemoryVertex> element = vertexMap.get(vertexId);
        if (element == null || !isIncludedInTimeSpan(element, fetchHints, endTime, authorizations)) {
            return null;
        } else {
            return element.createElement(this, fetchHints, endTime, authorizations);
        }
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(
            final String vertexIdPrefix, final FetchHints fetchHints,
            final Long endTime, final Authorizations authorizations
    ) {
        validateAuthorizations(authorizations);

        return new LookAheadIterable<InMemoryTableVertex, Vertex>() {
            @Override
            protected boolean isIncluded(InMemoryTableVertex element, Vertex vertex) {
                return vertex != null && SqlGraph.this.isIncluded(element, fetchHints, authorizations);
            }

            @Override
            protected Vertex convert(InMemoryTableVertex element) {
                return element.createElement(SqlGraph.this, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryTableVertex> createIterator() {
                Iterator<InMemoryTableElement<InMemoryVertex>> elements = vertexMap.query(
                        "id like ?",
                        vertexIdPrefix + "%"
                );

                return new ConvertingIterable<InMemoryTableElement<InMemoryVertex>, InMemoryTableVertex>(elements) {
                    @Override
                    protected InMemoryTableVertex convert(InMemoryTableElement<InMemoryVertex> element) {
                        return ((SqlTableVertex) element).asInMemoryTableElement();
                    }
                }.iterator();
            }
        };
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        InMemoryTableElement<InMemoryEdge> element = edgeMap.get(edgeId);
        if (element == null || !isIncluded(element, fetchHints, authorizations)) {
            return null;
        } else {
            return element.createElement(this, fetchHints, endTime, authorizations);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Vertex> getVertices(
            final Iterable<String> ids, final FetchHints fetchHints,
            final Long endTime, final Authorizations authorizations
    ) {
        return (Iterable<Vertex>) getElements(ids, fetchHints, endTime, authorizations, vertexMap);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Edge> getEdges(
            Iterable<String> ids,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        return (Iterable<Edge>) getElements(ids, fetchHints, endTime, authorizations, edgeMap);
    }

    private <T extends InMemoryElement> Iterable<?> getElements(
            Iterable<String> ids,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations,
            SqlMap<InMemoryTableElement<T>> sqlMap
    ) {
        return new LookAheadIterable<InMemoryTableElement, T>() {
            @Override
            protected boolean isIncluded(InMemoryTableElement srcElement, T destElement) {
                return destElement != null && SqlGraph.this.isIncluded(srcElement, fetchHints, authorizations);
            }

            @SuppressWarnings("unchecked")
            @Override
            protected T convert(InMemoryTableElement element) {
                return (T) element.createElement(SqlGraph.this, fetchHints, endTime, authorizations);
            }

            @SuppressWarnings("unused")
            @Override
            protected Iterator<InMemoryTableElement> createIterator() {
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
                    Iterator<InMemoryTableElement<T>> elements = sqlMap.query(
                            idWhere.toString(),
                            Iterables.toArray(ids, Object.class)
                    );

                    return new ConvertingIterable<InMemoryTableElement, InMemoryTableElement>(elements) {
                        @Override
                        protected InMemoryTableElement convert(InMemoryTableElement element) {
                            return ((SqlTableElement) element).asInMemoryTableElement();
                        }
                    }.iterator();
                }
            }
        };
    }

    @Override
    public Iterable<Edge> getEdgesFromVertex(
            String vertexId,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        return new LookAheadIterable<InMemoryTableEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryTableEdge element, Edge edge) {
                return edge != null && SqlGraph.this.isIncluded(element, fetchHints, authorizations);
            }

            @Override
            protected Edge convert(InMemoryTableEdge element) {
                return element.createElement(SqlGraph.this, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryTableEdge> createIterator() {
                Iterator<InMemoryTableElement<InMemoryEdge>> elements =
                        edgeMap.query("in_vertex_id = ? or out_vertex_id = ?", vertexId, vertexId);

                return new ConvertingIterable<InMemoryTableElement<InMemoryEdge>, InMemoryTableEdge>(elements) {
                    @Override
                    protected InMemoryTableEdge convert(InMemoryTableElement<InMemoryEdge> element) {
                        return ((SqlTableEdge) element).asInMemoryTableElement();
                    }
                }.iterator();
            }
        };
    }

    @Override
    protected void alterElementPropertyMetadata(
            InMemoryTableElement inMemoryTableElement,
            List<SetPropertyMetadata> setPropertyMetadatas,
            Authorizations authorizations
    ) {
        super.alterElementPropertyMetadata(inMemoryTableElement, setPropertyMetadatas, authorizations);
        ((Storable) inMemoryTableElement).store();
    }

    protected SqlStreamingPropertyTable getStreamingPropertyTable() {
        return streamingPropertyTable;
    }

    @Override
    protected StreamingPropertyValueRef saveStreamingPropertyValue(
            String elementId,
            String key,
            String name,
            Visibility visibility,
            long timestamp,
            StreamingPropertyValue value
    ) {
        return streamingPropertyTable.put(elementId, key, name, visibility, timestamp, value);
    }
}
