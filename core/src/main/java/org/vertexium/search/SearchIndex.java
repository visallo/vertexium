package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.mutation.*;
import org.vertexium.util.FutureDeprecation;

import java.util.Collection;
import java.util.Set;

public interface SearchIndex {

    @FutureDeprecation
    default void addElement(
        Graph graph,
        Element element,
        Set<String> additionalVisibilities,
        Set<String> additionalVisibilitiesToDelete,
        Authorizations authorizations
    ) {
        addElement(
            graph,
            element,
            additionalVisibilities,
            additionalVisibilitiesToDelete,
            authorizations.getUser()
        );
    }

    void addElement(
        Graph graph,
        Element element,
        Set<String> additionalVisibilities,
        Set<String> additionalVisibilitiesToDelete,
        User user
    );

    <TElement extends Element> void addOrUpdateElement(Graph graph, ElementMutation<TElement> mutation, User user);

    @FutureDeprecation
    default void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        addOrUpdateElement(graph, element.prepareMutation().deleteElement(), authorizations.getUser());
    }

    @FutureDeprecation
    default void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        addOrUpdateElement(graph, element.prepareMutation().markElementHidden(visibility), authorizations.getUser());
    }

    @FutureDeprecation
    default void markElementVisible(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        addOrUpdateElement(graph, element.prepareMutation().markElementVisible(visibility), authorizations.getUser());
    }

    @FutureDeprecation
    default void markPropertyHidden(Graph graph, Element element, Property property, Visibility visibility, Authorizations authorizations) {
        addOrUpdateElement(graph, element.prepareMutation().markPropertyHidden(property, visibility), authorizations.getUser());
    }

    @FutureDeprecation
    default void markPropertyVisible(Graph graph, Element element, Property property, Visibility visibility, Authorizations authorizations) {
        addOrUpdateElement(graph, element.prepareMutation().markPropertyVisible(property, visibility), authorizations.getUser());
    }

    /**
     * Default delete property simply calls deleteProperty in a loop. It is up to the SearchIndex implementation to decide
     * if a collective method can be made more efficient
     */
    @FutureDeprecation
    default void deleteProperties(Graph graph, Element element, Collection<PropertyDescriptor> propertyList, Authorizations authorizations) {
        deleteProperties(graph, element, propertyList, authorizations.getUser());
    }

    default void deleteProperties(Graph graph, Element element, Collection<PropertyDescriptor> propertyList, User user) {
        propertyList.forEach(p -> deleteProperty(graph, element, p, user));
    }

    @FutureDeprecation
    default void deleteProperty(Graph graph, Element element, PropertyDescriptor property, Authorizations authorizations) {
        deleteProperty(graph, element, property, authorizations.getUser());
    }

    void deleteProperty(Graph graph, Element element, PropertyDescriptor property, User user);

    @FutureDeprecation
    org.vertexium.query.GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    GraphQuery queryGraph(Graph graph, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations);

    MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations);

    Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations);

    SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, User user);

    void flush(Graph graph);

    void shutdown();

    void clearCache();

    boolean isFieldBoostSupported();

    void truncate(Graph graph);

    void drop(Graph graph);

    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    boolean isQuerySimilarToTextSupported();

    boolean isFieldLevelSecuritySupported();

    @FutureDeprecation
    default <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        Authorizations authorizations
    ) {
        alterElementVisibility(graph, elementMutation, oldVisibility, newVisibility, authorizations.getUser());
    }

    <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        User user
    );

    @FutureDeprecation
    default void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        Authorizations authorizations
    ) {
        addElementExtendedData(
            graph,
            elementLocation,
            extendedDatas,
            additionalExtendedDataVisibilities,
            additionalExtendedDataVisibilityDeletes,
            authorizations.getUser()
        );
    }

    void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        User user
    );

    @FutureDeprecation
    default void addExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataRow> extendedDatas,
        Authorizations authorizations
    ) {
        addExtendedData(graph, elementLocation, extendedDatas, authorizations.getUser());
    }

    void addExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataRow> extendedDatas,
        User user
    );

    @FutureDeprecation
    default void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations) {
        deleteExtendedData(graph, extendedDataRowId, authorizations.getUser());
    }

    void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, User user);

    @FutureDeprecation
    default void deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        Authorizations authorizations
    ) {
        deleteExtendedData(graph, elementLocation, tableName, row, columnName, key, visibility, authorizations.getUser());
    }

    void deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        User user
    );

    @FutureDeprecation
    default void addAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, Authorizations authorizations) {
        addAdditionalVisibility(graph, element, visibility, eventData, authorizations.getUser());
    }

    void addAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, User user);

    @FutureDeprecation
    default void deleteAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, Authorizations authorizations) {
        deleteAdditionalVisibility(graph, element, visibility, eventData, authorizations.getUser());
    }

    void deleteAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, User user);
}
