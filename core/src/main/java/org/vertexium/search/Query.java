package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.query.*;
import org.vertexium.scoring.ScoringStrategy;

import java.util.Collection;
import java.util.EnumSet;

public interface Query {
    QueryResults<Vertex> vertices();

    QueryResults<Vertex> vertices(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching vertices.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching vertices.
     * Hidden vertices are not included in the results.
     *
     * @return The ids of vertices that match this query.
     */
    QueryResults<String> vertexIds();

    /**
     * Execute the query and return the ids of all matching vertices.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching vertices.
     *
     * @param fetchHints Details about which data to fetch.
     * @return The ids of vertices that match this query.
     */
    QueryResults<String> vertexIds(EnumSet<IdFetchHint> fetchHints);

    QueryResults<Edge> edges();

    QueryResults<Edge> edges(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching edges.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching edges.
     * Hidden edges are not included in the results.
     *
     * @return The ids of edges that match this query.
     */
    QueryResults<String> edgeIds();

    /**
     * Execute the query and return the ids of all matching edges.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching edges.
     * Hidden edges are not included in the results.
     *
     * @param fetchHints Details about which data to fetch.
     * @return The ids of edges that match this query.
     */
    QueryResults<String> edgeIds(EnumSet<IdFetchHint> fetchHints);

    QueryResults<ExtendedDataRow> extendedDataRows();

    QueryResults<ExtendedDataRow> extendedDataRows(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching extended data rows.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching extended data rows.
     * Hidden extended data rows are not included in the results.
     *
     * @return The ids of extended data rows that match this query.
     */
    QueryResults<ExtendedDataRowId> extendedDataRowIds();

    /**
     * Execute the query and return the ids of all matching extended data rows.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching extended data rows.
     * Hidden extended data rows are not included in the results.
     *
     * @param fetchHints Details about which data to fetch.
     * @return The ids of extended data rows that match this query.
     */
    QueryResults<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> fetchHints);

    QueryResults<Element> elements();

    QueryResults<Element> elements(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching elements.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching elements.
     * Hidden elements are not included in the results.
     *
     * @return The ids of elements that match this query.
     */
    QueryResults<String> elementIds();

    /**
     * Execute the query and return the ids of all matching elements.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching elements.
     * Hidden elements are not included in the results.
     *
     * @return The ids of elements that match this query.
     */
    QueryResults<String> elementIds(EnumSet<IdFetchHint> fetchHints);

    QueryResults<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, FetchHints fetchHints);

    QueryResults<? extends VertexiumObject> search();

    /**
     * Queries for properties in the given range.
     *
     * @param propertyName Name of property.
     * @param startValue   Inclusive start value.
     * @param endValue     Inclusive end value.
     * @return this
     */
    <T> Query range(String propertyName, T startValue, T endValue);

    /**
     * Queries for properties in the given range.
     *
     * @param propertyName        Name of property.
     * @param startValue          Inclusive start value.
     * @param inclusiveStartValue true, to include the start value
     * @param endValue            Inclusive end value.
     * @param inclusiveEndValue   true, to include the end value
     * @return this
     */
    <T> Query range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue);

    /**
     * Adds an id filter to the query. When searching for elements this will limit the search to the elements with
     * the given ids. When searching for extended data rows this will limit the search to the elements containing the
     * extended data rows.
     *
     * @param ids The ids to filter on.
     * @return The query object, allowing you to chain methods.
     */
    Query hasId(String... ids);

    /**
     * Adds an id filter to the query. When searching for elements this will limit the search to the elements with
     * the given ids. When searching for extended data rows this will limit the search to the elements containing the
     * extended data rows.
     *
     * @param ids The ids to filter on.
     * @return The query object, allowing you to chain methods.
     */
    Query hasId(Iterable<String> ids);

    /**
     * Adds a edge label filter to the query.
     *
     * @param edgeLabels The edge labels to filter on.
     * @return The query object, allowing you to chain methods.
     */
    Query hasEdgeLabel(String... edgeLabels);

    /**
     * Adds a edge label filter to the query.
     *
     * @param edgeLabels The edge labels to filter on.
     * @return The query object, allowing you to chain methods.
     */
    Query hasEdgeLabel(Collection<String> edgeLabels);

    /**
     * Adds an authorization filter to the query.
     *
     * @param authorizations An element will match if it or any of its properties use one of the specified authorizations.
     * @return The query object, allowing you to chain methods.
     */
    Query hasAuthorization(String... authorizations);

    /**
     * Adds an authorization filter to the query.
     *
     * @param authorizations An element will match if it or any of its properties use one of the specified authorizations.
     * @return The query object, allowing you to chain methods.
     */
    Query hasAuthorization(Iterable<String> authorizations);

    /**
     * Adds a extended data element filter to the query. This will query any table.
     *
     * @param elementType The type of element.
     * @param elementId   The element id
     * @return The query object, allowing you to chain methods.
     */
    Query hasExtendedData(ElementType elementType, String elementId);

    /**
     * Adds a extended data element filter to the query. This will limit the search to the supplied extended data
     * records.
     *
     * @param elementType The type of element. null to search all element types.
     * @param elementId   The element id. null to search all elements.
     * @param tableName   The table name. null to search all tables.
     * @return The query object, allowing you to chain methods.
     */
    Query hasExtendedData(ElementType elementType, String elementId, String tableName);

    /**
     * Adds a multiple extended data element filter to the query. These will be or'ed together. This will limit the
     * search to the supplied extended data records.
     *
     * @param filters The list of filters to be or'ed together.
     * @return The query object, allowing you to chain methods.
     */
    Query hasExtendedData(Iterable<HasExtendedDataFilter> filters);

    /**
     * Adds a extended data table filter to the query.
     *
     * @param tableName The table name
     * @return The query object, allowing you to chain methods.
     */
    Query hasExtendedData(String tableName);

    /**
     * Adds an {@link Compare#EQUAL} filter to the query.
     *
     * @param propertyName The name of the property to query on.
     * @param value        The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query has(String propertyName, T value);

    /**
     * Adds an {@link Contains#NOT_IN} filter to the query.
     *
     * @param propertyName The name of the property to query on.
     * @param value        The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query hasNot(String propertyName, T value);

    /**
     * Adds a has filter to the query.
     *
     * @param propertyName The name of the property the element must contain.
     * @return The query object, allowing you to chain methods.
     */
    Query has(String propertyName);

    /**
     * Adds a filter to the query.
     *
     * @param propertyNames A document will match if it contains any properties specified.
     * @return The query object, allowing you to chain methods.
     */
    Query has(Iterable<String> propertyNames);

    /**
     * Adds a hasNot filter to the query.
     *
     * @param propertyName The name of the property the element must not contain.
     * @return The query object, allowing you to chain methods.
     */
    Query hasNot(String propertyName);

    /**
     * Adds a filter to the query.
     *
     * @param propertyNames A document will match if it does not contain any properties specified.
     * @return The query object, allowing you to chain methods.
     */
    Query hasNot(Iterable<String> propertyNames);

    /**
     * Adds a filter to the query.
     *
     * @param propertyName The name of the property to query on.
     * @param predicate    One of {@link Compare},
     *                     {@link TextPredicate},
     *                     or {@link GeoCompare}.
     * @param value        The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query has(String propertyName, Predicate predicate, T value);

    /**
     * Adds a filter to the query.
     *
     * @param propertyNames All properties to query on. A match in any of the given properties will cause a document to match.
     * @param predicate     One of {@link Compare},
     *                      {@link TextPredicate},
     *                      or {@link GeoCompare}.
     * @param value         The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query has(Iterable<String> propertyNames, Predicate predicate, T value);

    /**
     * Adds a filter to the query.
     *
     * @param dataType  All properties with a matching data type will be queried on. A match in any of the selected properties will cause a document to match.
     * @param predicate One of {@link Compare},
     *                  {@link TextPredicate},
     *                  or {@link GeoCompare}.
     * @param value     The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query has(Class dataType, Predicate predicate, T value);


    /**
     * Adds a has filter to the query.
     *
     * @param dataType A document will match if it contains any properties of the specified data type.
     * @return The query object, allowing you to chain methods.
     */
    Query has(Class dataType);

    /**
     * Adds a hasNot filter to the query.
     *
     * @param dataType A document will match if it does not contain any properties of the specified data type.
     * @return The query object, allowing you to chain methods.
     */
    Query hasNot(Class dataType);

    /**
     * Skips the given number of items.
     */
    Query skip(int count);

    /**
     * Limits the number of items returned. null will return all elements.
     */
    Query limit(Integer count);

    /**
     * Limits the number of items returned. null will return all elements.
     */
    Query limit(Long count);

    /**
     * Minimum score to return
     */
    Query minScore(double score);

    /**
     * Sort the results by the given property name.
     *
     * @param propertyName The property to sort by.
     * @param direction    The direction to sort.
     * @return The query object, allowing you to chain methods.
     */
    Query sort(String propertyName, SortDirection direction);

    /**
     * Test to see if aggregation is supported.
     *
     * @param aggregation the aggregation to test.
     * @return true, if the aggregation is supported
     */
    boolean isAggregationSupported(Aggregation aggregation);

    /**
     * Add an aggregation to the query
     *
     * @param aggregation the aggregation to add.
     * @return The query object, allowing you to chain methods.
     */
    Query addAggregation(Aggregation aggregation);

    /**
     * Gets the added aggregations
     */
    Iterable<Aggregation> getAggregations();

    /**
     * Sets the scoring strategy for this query
     */
    Query scoringStrategy(ScoringStrategy scoringStrategy);
}
