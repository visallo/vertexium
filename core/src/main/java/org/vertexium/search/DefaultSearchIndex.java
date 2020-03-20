package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityAddMutation;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityDeleteMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.query.*;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.vertexium.util.Preconditions.checkNotNull;

public class DefaultSearchIndex implements SearchIndex {
    @SuppressWarnings("unused")
    public DefaultSearchIndex(GraphConfiguration configuration) {

    }

    @Override
    public CompletableFuture<Void> addElement(
        Graph graph,
        Element element,
        Set<String> additionalVisibilities,
        Set<String> additionalVisibilitiesToDelete,
        Authorizations authorizations
    ) {
        checkNotNull(element, "element cannot be null");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <TElement extends Element> void updateElement(Graph graph, ExistingElementMutation<TElement> mutation, Authorizations authorizations) {
        checkNotNull(mutation, "mutation cannot be null");
    }

    @Override
    public void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markElementVisible(
        Graph graph,
        ElementLocation elementLocation,
        Visibility visibility,
        Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markPropertyHidden(
        Graph graph,
        ElementLocation elementLocation,
        Property property,
        Visibility visibility,
        Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(property, "property cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markPropertyVisible(
        Graph graph,
        ElementLocation elementLocation,
        Property property,
        Visibility visibility,
        Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(property, "property cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        Authorizations authorizations
    ) {
        checkNotNull(elementMutation, "elementMutation cannot be null");
        checkNotNull(newVisibility, "newVisibility cannot be null");
    }

    @Override
    public void deleteElement(Graph graph, ElementId element, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public void deleteElements(Graph graph, Iterable<? extends ElementId> elementIds, Authorizations authorizations) {
        checkNotNull(elementIds, "element cannot be null");
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations) {
        for (Element element : elements) {
            addElement(graph, element, element.getAdditionalVisibilities(), null, authorizations);
        }
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new DefaultGraphQuery(graph, queryString, authorizations);
    }

    @Override
    public MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations) {
        return new DefaultMultiVertexQuery(graph, vertexIds, queryString, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryString, authorizations);
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
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations) {
        throw new VertexiumException("querySimilarTo not supported");
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    @Override
    public boolean isDeleteElementSupported() {
        return false;
    }

    @Override
    public CompletableFuture<Void> addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        Authorizations authorizations
    ) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataRow> extendedDatas,
        Authorizations authorizations
    ) {
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations) {
        checkNotNull(extendedDataRowId, "extendedDataRowId cannot be null");
    }

    @Override
    public CompletableFuture<Void> deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        checkNotNull(row, "row cannot be null");
        checkNotNull(columnName, "columnName cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations) {
        return new DefaultExtendedDataQuery(graph, element, tableName, queryString, authorizations);
    }
}