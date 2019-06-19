package org.vertexium.inmemory.search;

import org.vertexium.*;
import org.vertexium.mutation.*;
import org.vertexium.query.DefaultExtendedDataQuery;
import org.vertexium.search.SearchIndex;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultSearchIndex implements SearchIndex {
    @SuppressWarnings("unused")
    public DefaultSearchIndex(GraphConfiguration configuration) {

    }

    @Override
    public void addElement(
        Graph graph,
        Element element,
        Set<String> additionalVisibilities,
        Set<String> additionalVisibilitiesToDelete,
        User user
    ) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public <TElement extends Element> void addOrUpdateElement(Graph graph, ElementMutation<TElement> mutation, User user) {
        checkNotNull(mutation, "mutation cannot be null");
    }

    @Override
    public <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        User user
    ) {
        checkNotNull(elementMutation, "elementMutation cannot be null");
        checkNotNull(newVisibility, "newVisibility cannot be null");
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, User user) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public org.vertexium.query.GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new org.vertexium.query.DefaultGraphQuery(graph, queryString, authorizations);
    }

    @Override
    public org.vertexium.search.GraphQuery queryGraph(Graph graph, String queryString, User user) {
        return new DefaultGraphQuery(graph, queryString, user);
    }

    @Override
    public org.vertexium.query.MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations) {
        return new org.vertexium.query.DefaultMultiVertexQuery(graph, vertexIds, queryString, authorizations);
    }

    @Override
    public org.vertexium.search.MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, User user) {
        return new DefaultMultiVertexQuery(graph, vertexIds, queryString, user);
    }

    @Override
    public org.vertexium.query.VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new org.vertexium.query.DefaultVertexQuery(graph, vertex, queryString, authorizations);
    }

    @Override
    public org.vertexium.search.VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, User user) {
        return new DefaultVertexQuery(graph, vertex, queryString, user);
    }

    @Override
    public void flush(Graph graph) {
    }

    @Override
    public void clearCache() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    @Override
    public void truncate(Graph graph) {
    }

    @Override
    public void drop(Graph graph) {
    }

    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.PROPERTY;
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return false;
    }

    @Override
    public org.vertexium.query.SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations) {
        throw new VertexiumException("querySimilarTo not supported");
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, User user) {
        throw new VertexiumException("querySimilarTo not supported");
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    @Override
    public void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        User user
    ) {
    }

    @Override
    public void addExtendedData(Graph graph, ElementLocation elementLocation, Iterable<ExtendedDataRow> extendedDatas, User user) {
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, User user) {
        checkNotNull(extendedDataRowId, "extendedDataRowId cannot be null");
    }

    @Override
    public void deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        User user
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        checkNotNull(row, "row cannot be null");
        checkNotNull(columnName, "columnName cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void addAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, User user) {
    }

    @Override
    public void deleteAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, User user) {
    }

    @Override
    public org.vertexium.query.Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations) {
        return new DefaultExtendedDataQuery(graph, element, tableName, queryString, authorizations);
    }

    @Override
    public org.vertexium.search.Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, User user) {
        throw new VertexiumException("Not Yet Implemented");
    }
}