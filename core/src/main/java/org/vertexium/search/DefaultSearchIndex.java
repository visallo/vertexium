package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.query.*;

import static org.vertexium.util.Preconditions.checkNotNull;

public class DefaultSearchIndex implements SearchIndex {
    @SuppressWarnings("unused")
    public DefaultSearchIndex(GraphConfiguration configuration) {

    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
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
    public void markElementVisible(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markPropertyHidden(Graph graph, Element element, Property property, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
        checkNotNull(property, "property cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markPropertyVisible(Graph graph, Element element, Property property, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
        checkNotNull(property, "property cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void alterElementVisibility(Graph graph, Element element, Visibility oldVisibility, Visibility newVisibility, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
        checkNotNull(newVisibility, "newVisibility cannot be null");
    }

    @Override
    public void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations) {
        for (Element element : elements) {
            addElement(graph, element, authorizations);
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
    public void addElementExtendedData(Graph graph, Element element, Iterable<ExtendedDataMutation> extendedDatas, Authorizations authorizations) {
    }

    @Override
    public void addExtendedData(Graph graph, Iterable<ExtendedDataRow> extendedDatas, Authorizations authorizations) {
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations) {
        checkNotNull(extendedDataRowId, "extendedDataRowId cannot be null");
    }

    @Override
    public void deleteExtendedData(
            Graph graph,
            Element element,
            String tableName,
            String row,
            String columnName,
            String key,
            Visibility visibility,
            Authorizations authorizations
    ) {
        checkNotNull(element, "element cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        checkNotNull(row, "row cannot be null");
        checkNotNull(columnName, "columnName cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations) {
        return new DefaultExtendedDataQuery(graph, element, tableName, queryString, authorizations);
    }
}