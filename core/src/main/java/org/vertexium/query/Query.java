package org.vertexium.query;

import org.vertexium.*;

import java.util.Collection;
import java.util.EnumSet;

public interface Query {
    QueryResultsIterable<Vertex> vertices();

    QueryResultsIterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<String> vertexIds();

    QueryResultsIterable<Edge> edges();

    QueryResultsIterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<String> edgeIds();

    QueryResultsIterable<ExtendedDataRow> extendedDataRows();

    QueryResultsIterable<ExtendedDataRow> extendedDataRows(EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds();

    @Deprecated
    QueryResultsIterable<Edge> edges(String label);

    @Deprecated
    QueryResultsIterable<Edge> edges(String label, EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<Element> elements();

    QueryResultsIterable<Element> elements(EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<String> elementIds();

    QueryResultsIterable<? extends VertexiumObject> search(EnumSet<VertexiumObjectType> objectTypes, EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<? extends VertexiumObject> search();

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
     * Adds an id filter to the query.
     *
     * @param ids The ids to filter on.
     * @return The query object, allowing you to chain methods.
     */
    Query hasId(String... ids);

    /**
     * Adds an id filter to the query.
     *
     * @param ids The ids to filter on.
     * @return The query object, allowing you to chain methods.
     */
    Query hasId(Collection<String> ids);

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
     * Adds a hasNot filter to the query.
     *
     * @param propertyName The name of the property the element must not contain.
     * @return The query object, allowing you to chain methods.
     */
    Query hasNot(String propertyName);

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
}
