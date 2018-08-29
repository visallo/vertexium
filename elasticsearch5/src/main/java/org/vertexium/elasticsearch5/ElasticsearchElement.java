package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.QueryableIterable;

public abstract class ElasticsearchElement extends ElementBase {
    private String className = ElasticsearchElement.class.getSimpleName();
    private final Graph graph;
    private FetchHints fetchHints;
    private String id;
    private Authorizations authorizations;

    public ElasticsearchElement(Graph graph,
                                String id,
                                FetchHints fetchHints,
                                Authorizations authorizations) {
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
        throw new VertexiumNotSupportedException("getProperties is not supported on " + className);
    }

    @Override
    public Property getProperty(String name) {
        throw new VertexiumNotSupportedException("getProperty is not supported on " + className);
    }

    @Override
    public Object getPropertyValue(String name) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported on " + className);
    }

    @Override
    public Property getProperty(String key, String name) {
        throw new VertexiumNotSupportedException("getProperty is not supported on " + className);
    }

    @Override
    public Iterable<Object> getPropertyValues(String name) {
        throw new VertexiumNotSupportedException("getPropertyValues is not supported on " + className);
    }

    @Override
    public Iterable<Object> getPropertyValues(String key, String name) {
        throw new VertexiumNotSupportedException("getPropertyValues is not supported on " + className);
    }

    @Override
    public Object getPropertyValue(String key, String name) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported on " + className);
    }

    @Override
    public Object getPropertyValue(String name, int index) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported on " + className);
    }

    @Override
    public Object getPropertyValue(String key, String name, int index) {
        throw new VertexiumNotSupportedException("getPropertyValue is not supported on " + className);
    }

    @Override
    public Visibility getVisibility() {
        throw new VertexiumNotSupportedException("getVisibility is not supported on " + className);
    }

    @Override
    public long getTimestamp() {
        throw new VertexiumNotSupportedException("getTimestamp is not supported on " + className);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getHistoricalPropertyValues is not supported on " + className);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Long startTime, Long endTime, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getHistoricalPropertyValues is not supported on " + className);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getHistoricalPropertyValues is not supported on " + className);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("getHistoricalPropertyValues is not supported on " + className);
    }

    @Override
    public <T extends Element> ExistingElementMutation<T> prepareMutation() {
        throw new VertexiumNotSupportedException("prepareMutation is not supported on " + className);
    }

    @Override
    public void deleteProperty(String key, String name, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("deleteProperty is not supported on " + className);
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("deleteProperty is not supported on " + className);
    }

    @Override
    public void deleteProperties(String name, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("deleteProperties is not supported on " + className);
    }

    @Override
    public void softDeleteProperty(String key, String name, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("softDeleteProperty is not supported on " + className);
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("softDeleteProperty is not supported on " + className);
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("softDeleteProperties is not supported on " + className);
    }

    @Override
    public GraphWithSearchIndex getGraph() {
        return (GraphWithSearchIndex) graph;
    }

    @Override
    public void addPropertyValue(String key, String name, Object value, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("addPropertyValue is not supported on " + className);
    }

    @Override
    public void addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("addPropertyValue is not supported on " + className);
    }

    @Override
    public void setProperty(String name, Object value, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("setProperty is not supported on " + className);
    }

    @Override
    public void setProperty(String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("setProperty is not supported on " + className);
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyHidden is not supported on " + className);
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyHidden is not supported on " + className);
    }

    @Override
    public void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyHidden is not supported on " + className);
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyHidden is not supported on " + className);
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyVisible is not supported on " + className);
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyVisible is not supported on " + className);
    }

    @Override
    public void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyVisible is not supported on " + className);
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumNotSupportedException("markPropertyVisible is not supported on " + className);
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        throw new VertexiumNotSupportedException("isHidden is not supported on " + className);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        throw new VertexiumNotSupportedException("getHiddenVisibilities is not supported on " + className);
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        throw new VertexiumNotSupportedException("getExtendedDataTableNames is not supported on " + className);
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName) {
        throw new VertexiumNotSupportedException("getExtendedData is not supported on " + className);
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }
}
