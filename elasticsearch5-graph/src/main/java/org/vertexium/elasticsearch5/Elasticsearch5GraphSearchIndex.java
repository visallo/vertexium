package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.mutation.*;
import org.vertexium.query.*;
import org.vertexium.search.SearchIndex;

import java.util.Set;

public class Elasticsearch5GraphSearchIndex implements SearchIndex {
    private final Elasticsearch5Graph graph;

    public Elasticsearch5GraphSearchIndex(Elasticsearch5Graph graph) {
        this.graph = graph;
    }

    @Override
    public void addElement(Graph graph, Element element, Set<Visibility> additionalVisibilities, Set<Visibility> additionalVisibilitiesToDelete, User user) {
        element.prepareMutation()
            .save(user);
    }

    @Override
    public <TElement extends Element> void addOrUpdateElement(Graph graph, ElementMutation<TElement> mutation, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        Elasticsearch5GraphConfiguration config = this.graph.getConfiguration();
        return new ElasticsearchSearchGraphQuery(
            this.graph.getClient(),
            graph,
            this.graph.getIndexService(),
            this.graph.getPropertyNameService(),
            this.graph.getPropertyNameVisibilityStore(),
            this.graph.getIdStrategy(),
            queryString,
            new ElasticsearchSearchQueryBase.Options()
                .setIndexSelectionStrategy(this.graph.getIndexSelectionStrategy())
                .setPageSize(config.getQueryPageSize())
                .setPagingLimit(config.getPagingLimit())
                .setScrollKeepAlive(config.getScrollKeepAlive())
                .setTermAggregationShardSize(config.getTermAggregationShardSize())
                .setMaxQueryStringTerms(config.getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations) {
        Elasticsearch5GraphConfiguration config = this.graph.getConfiguration();
        return new ElasticsearchSearchExtendedDataQuery(
            this.graph.getClient(),
            graph,
            this.graph.getIndexService(),
            this.graph.getPropertyNameService(),
            this.graph.getPropertyNameVisibilityStore(),
            this.graph.getIdStrategy(),
            queryString,
            element.getId(),
            tableName,
            new ElasticsearchSearchExtendedDataQuery.Options()
                .setIndexSelectionStrategy(this.graph.getIndexSelectionStrategy())
                .setPageSize(config.getQueryPageSize())
                .setPagingLimit(config.getPagingLimit())
                .setScrollKeepAlive(config.getScrollKeepAlive())
                .setTermAggregationShardSize(config.getTermAggregationShardSize())
                .setMaxQueryStringTerms(config.getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public org.vertexium.search.GraphQuery queryGraph(Graph graph, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations) {
        Elasticsearch5GraphConfiguration config = this.graph.getConfiguration();
        return new ElasticsearchSearchMultiVertexQuery(
            this.graph.getClient(),
            graph,
            this.graph.getIndexService(),
            this.graph.getPropertyNameService(),
            this.graph.getPropertyNameVisibilityStore(),
            this.graph.getIdStrategy(),
            queryString,
            vertexIds,
            new ElasticsearchSearchQueryBase.Options()
                .setIndexSelectionStrategy(this.graph.getIndexSelectionStrategy())
                .setPageSize(config.getQueryPageSize())
                .setPagingLimit(config.getPagingLimit())
                .setScrollKeepAlive(config.getScrollKeepAlive())
                .setTermAggregationShardSize(config.getTermAggregationShardSize())
                .setMaxQueryStringTerms(config.getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public org.vertexium.search.MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return ((Elasticsearch5Graph) graph).queryVertex(graph, vertex, queryString, authorizations);
    }

    @Override
    public org.vertexium.search.VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public org.vertexium.search.Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void flush(Graph graph) {
        // let graph handle this
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void clearCache() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    @Override
    public void truncate(Graph graph) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void drop(Graph graph) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return true;
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    @Override
    public <T extends Element> void alterElementVisibility(Graph graph, ExistingElementMutation<T> elementMutation, Visibility oldVisibility, Visibility newVisibility, User user) {
        // handled by graph
    }

    @Override
    public void addElementExtendedData(Graph graph, ElementLocation elementLocation, Iterable<ExtendedDataMutation> extendedDatas, Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities, Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes, User user) {
        // handled by graph
    }

    @Override
    public void addExtendedData(Graph graph, ElementLocation elementLocation, Iterable<ExtendedDataRow> extendedDatas, User user) {
        // handled by graph
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, User user) {
        // handled by graph
    }

    @Override
    public void deleteExtendedData(Graph graph, ElementLocation elementLocation, String tableName, String row, String columnName, String key, Visibility visibility, User user) {
        // handled by graph
    }

    @Override
    public void addAdditionalVisibility(Graph graph, Element element, Visibility visibility, Object eventData, User user) {
        // handled by graph
    }

    @Override
    public void deleteAdditionalVisibility(Graph graph, Element element, Visibility visibility, Object eventData, User user) {
        // handled by graph
    }
}
