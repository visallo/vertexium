package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.MultiVertexQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.query.VertexQuery;

import java.util.Collection;

public interface SearchIndex {
    void addElement(Graph graph, Element element, Authorizations authorizations);

    void deleteElement(Graph graph, Element element, Authorizations authorizations);

    /**
     * Default delete property simply calls deleteProperty in a loop. It is up to the SearchIndex implementation to decide
     * if a collective method can be made more efficient
     */
    default void deleteProperties(
            Graph graph,
            Element element,
            Collection<PropertyDescriptor> propertyList,
            Authorizations authorizations) {
        propertyList.forEach(p -> deleteProperty(graph, element, p, authorizations));
    }

    void deleteProperty(
            Graph graph,
            Element element,
            PropertyDescriptor property,
            Authorizations authorizations
    );

    void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations);

    GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations);

    void flush(Graph graph);

    void shutdown();

    boolean isFieldBoostSupported();

    void truncate(Graph graph);

    void drop(Graph graph);

    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    boolean isQuerySimilarToTextSupported();

    SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations);

    boolean isFieldLevelSecuritySupported();

    void alterElementVisibility(Graph graph, Element element, Visibility oldVisibility, Visibility newVisibility, Authorizations authorizations);

    void addElementExtendedData(Graph graph, Element element, Iterable<ExtendedDataMutation> extendedDatas, Authorizations authorizations);

    void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations);
}
