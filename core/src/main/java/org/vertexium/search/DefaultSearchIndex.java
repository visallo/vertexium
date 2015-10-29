package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.query.*;

public class DefaultSearchIndex implements SearchIndex {
    @SuppressWarnings("unused")
    public DefaultSearchIndex(GraphConfiguration configuration) {

    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {

    }

    @Override
    public void deleteElement(Graph graph, Element element, Authorizations authorizations) {

    }

    @Override
    public void deleteProperty(Graph graph, Element element, Property property, Authorizations authorizations) {

    }

    @Override
    public void deleteProperty(Graph graph, Element element, String propertyKey, String propertyName, Visibility propertyVisibility, Authorizations authorizations) {

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
    public void flush() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    @Override
    public void truncate() {
    }

    @Override
    public void drop() {

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
}