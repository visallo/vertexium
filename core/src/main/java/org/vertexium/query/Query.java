package org.vertexium.query;

import org.vertexium.Edge;
import org.vertexium.Element;
import org.vertexium.FetchHint;
import org.vertexium.Vertex;

import java.util.EnumSet;

public interface Query {
    Iterable<Vertex> vertices();

    Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    Iterable<Edge> edges();

    Iterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    Iterable<Edge> edges(String label);

    Iterable<Edge> edges(String label, EnumSet<FetchHint> fetchHints);

    Iterable<Element> elements();

    Iterable<Element> elements(EnumSet<FetchHint> fetchHints);

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
     * Limits the number of items returned.
     */
    Query limit(int count);

    /**
     * Sort the results by the given property name.
     *
     * @param propertyName The property to sort by.
     * @param direction    The direction to sort.
     * @return The query object, allowing you to chain methods.
     */
    Query sort(String propertyName, SortDirection direction);
}
