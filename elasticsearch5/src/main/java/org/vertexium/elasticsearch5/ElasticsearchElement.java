package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.QueryableIterable;

import java.util.stream.Stream;

public abstract class ElasticsearchElement extends ElementBase {
    private final Graph graph;
    private FetchHints fetchHints;
    private String id;
    private Authorizations authorizations;

    public ElasticsearchElement(
        Graph graph,
        String id,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        this.id = id;
        this.graph = graph;
        this.fetchHints = fetchHints;
        this.authorizations = authorizations;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Iterable<Property> getProperties() {
        throw new VertexiumNotSupportedException("getProperties is not supported");
    }

    @Override
    public Property getProperty(String name) {
        throw new VertexiumNotSupportedException("getProperty is not supported");
    }

    @Override
    public Object getPropertyValue(String name) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Property getProperty(String key, String name) {
        throw new VertexiumNotSupportedException("getProperty is not supported");
    }

    @Override
    public Iterable<Object> getPropertyValues(String name) {
        throw new VertexiumNotSupportedException("getPropertyValues is not supported");
    }

    @Override
    public Iterable<Object> getPropertyValues(String key, String name) {
        throw new VertexiumNotSupportedException("getPropertyValues is not supported");
    }

    @Override
    public Object getPropertyValue(String key, String name) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Object getPropertyValue(String name, int index) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Object getPropertyValue(String key, String name, int index) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Visibility getVisibility() {
        throw new VertexiumNotSupportedException("getVisibility is not supported");
    }

    @Override
    public long getTimestamp() {
        throw new VertexiumNotSupportedException("getTimestamp is not supported");
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        Authorizations authorizations
    ) {
        throw new VertexiumNotSupportedException("getHistoricalEvents is not supported");
    }

    @Override
    public <T extends Element> ExistingElementMutation<T> prepareMutation() {
        throw new VertexiumNotSupportedException("prepareMutation is not supported");
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        throw new VertexiumNotSupportedException("getHiddenVisibilities is not supported");
    }

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        throw new VertexiumNotSupportedException("getAdditionalVisibilities is not supported");
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        throw new VertexiumNotSupportedException("getExtendedDataTableNames is not supported");
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        throw new VertexiumNotSupportedException("getExtendedData is not supported");
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }
}
