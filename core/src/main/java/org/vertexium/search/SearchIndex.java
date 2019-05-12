package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityAddMutation;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityDeleteMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.query.*;

import java.util.Collection;

public interface SearchIndex {
    void addElement(Graph graph, Element element, Authorizations authorizations);

    <TElement extends Element> void updateElement(Graph graph, ExistingElementMutation<TElement> mutation, Authorizations authorizations);

    void deleteElement(Graph graph, Element element, Authorizations authorizations);

    void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations);

    void markElementVisible(
        Graph graph,
        ElementLocation elementLocation,
        Visibility visibility,
        Authorizations authorizations
    );

    void markPropertyHidden(
        Graph graph,
        ElementLocation elementLocation,
        Property property,
        Visibility visibility,
        Authorizations authorizations
    );

    void markPropertyVisible(
        Graph graph,
        ElementLocation elementLocation,
        Property property,
        Visibility visibility,
        Authorizations authorizations
    );

    /**
     * Default delete property simply calls deleteProperty in a loop. It is up to the SearchIndex implementation to decide
     * if a collective method can be made more efficient
     */
    default void deleteProperties(
        Graph graph,
        Element element,
        Collection<PropertyDescriptor> propertyList,
        Authorizations authorizations
    ) {
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

    Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations);

    SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations);

    void flush(Graph graph);

    void shutdown();

    void clearCache();

    boolean isFieldBoostSupported();

    void truncate(Graph graph);

    void drop(Graph graph);

    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    boolean isQuerySimilarToTextSupported();

    boolean isFieldLevelSecuritySupported();

    <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        Authorizations authorizations
    );

    void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        Authorizations authorizations
    );

    void addExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataRow> extendedDatas,
        Authorizations authorizations
    );

    void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations);

    void deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        Authorizations authorizations
    );

    void addAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, Authorizations authorizations);

    void deleteAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, Authorizations authorizations);
}
