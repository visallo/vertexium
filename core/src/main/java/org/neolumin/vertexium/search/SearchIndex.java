package org.neolumin.vertexium.search;

import org.neolumin.vertexium.*;
import org.neolumin.vertexium.query.GraphQuery;
import org.neolumin.vertexium.query.SimilarToGraphQuery;
import org.neolumin.vertexium.query.VertexQuery;

import java.io.IOException;

public interface SearchIndex {
    void addElement(Graph graph, Element element, Authorizations authorizations);

    void removeElement(Graph graph, Element element, Authorizations authorizations);

    void removeProperty(Graph graph, Element element, Property property, Authorizations authorizations);

    void removeProperty(
            Graph graph,
            Element element,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            Authorizations authorizations
    );

    void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations);

    GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations);

    void flush();

    void shutdown();

    void addPropertyDefinition(PropertyDefinition propertyDefinition) throws IOException;

    boolean isFieldBoostSupported();

    void clearData();

    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    boolean isQuerySimilarToTextSupported();

    SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations);
}
