package org.vertexium.query;

import org.vertexium.Edge;
import org.vertexium.Element;
import org.vertexium.FetchHint;
import org.vertexium.Vertex;

import java.util.Collection;
import java.util.EnumSet;

public interface Query {
    QueryResultsIterable<Vertex> vertices();

    QueryResultsIterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<Edge> edges();

    QueryResultsIterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    @Deprecated
    QueryResultsIterable<Edge> edges(String label);

    @Deprecated
    QueryResultsIterable<Edge> edges(String label, EnumSet<FetchHint> fetchHints);

    QueryResultsIterable<Element> elements();

    QueryResultsIterable<Element> elements(EnumSet<FetchHint> fetchHints);

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
     * Adds a edge label filter to the query.
     *
     * @param edgeLabels The edge labels to filter on.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query hasEdgeLabel(String... edgeLabels);

    /**
     * Adds a edge label filter to the query.
     *
     * @param edgeLabels The edge labels to filter on.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query hasEdgeLabel(Collection<String> edgeLabels);

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
    Query hasId(Iterable<String> ids);

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
