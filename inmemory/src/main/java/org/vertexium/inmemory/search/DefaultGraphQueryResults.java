package org.vertexium.inmemory.search;

import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.*;
import org.vertexium.scoring.ScoringStrategy;
import org.vertexium.search.QueryResults;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.Preconditions.checkNotNull;

public class DefaultGraphQueryResults<T, U extends VertexiumObject> implements QueryResults<T> {
    private final QueryParameters parameters;
    private final boolean evaluateQueryString;
    private final boolean evaluateHasContainers;
    private final List<U> allHits;
    private final Collection<Aggregation> aggregations;
    private final Function<U, T> transform;

    public DefaultGraphQueryResults(
            QueryParameters parameters,
            Stream<U> objects,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            Function<U, T> transform
    ) {
        this(parameters, objects, evaluateQueryString, evaluateHasContainers, evaluateSortContainers, null, transform);
    }

    public DefaultGraphQueryResults(
            QueryParameters parameters,
            Stream<U> objects,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            Collection<Aggregation> aggregations,
            Function<U, T> transform
    ) {
        checkNotNull(objects, "objects cannot be null");
        this.parameters = parameters;
        this.evaluateQueryString = evaluateQueryString;
        this.evaluateHasContainers = evaluateHasContainers;
        this.aggregations = aggregations;
        this.transform = transform;
        if (evaluateSortContainers && this.parameters.getSortContainers().size() > 0) {
            objects = sortUsingSortContainers(objects, parameters.getSortContainers());
        } else if (evaluateHasContainers && this.parameters.getScoringStrategy() != null) {
            objects = sortUsingScoringStrategy(objects, parameters.getScoringStrategy());
        }

        allHits = objects.filter(this::isMatch).collect(Collectors.toList());
    }

    private Stream<U> sortUsingScoringStrategy(Stream<U> objects, ScoringStrategy scoringStrategy) {
        return objects.sorted(new ScoringStrategyComparator<>(scoringStrategy));
    }

    private Stream<U> sortUsingSortContainers(Stream<U> objects, List<QueryBase.SortContainer> sortContainers) {
        return objects.sorted(new SortContainersComparator<>(sortContainers));
    }

    private boolean isMatch(VertexiumObject vertexiumElem) {
        if (evaluateHasContainers && vertexiumElem != null) {
            for (QueryBase.HasContainer has : parameters.getHasContainers()) {
                if (!has.isMatch(vertexiumElem)) {
                    return false;
                }
            }
            if (vertexiumElem instanceof Edge && parameters.getEdgeLabels().size() > 0) {
                Edge edge = (Edge) vertexiumElem;
                if (!parameters.getEdgeLabels().contains(edge.getLabel())) {
                    return false;
                }
            }
            if (parameters.getIds() != null) {
                if (vertexiumElem instanceof Element) {
                    if (!parameters.getIds().contains(((Element) vertexiumElem).getId())) {
                        return false;
                    }
                } else if (vertexiumElem instanceof ExtendedDataRow) {
                    if (!parameters.getIds().contains(((ExtendedDataRow) vertexiumElem).getId().getElementId())) {
                        return false;
                    }
                } else {
                    throw new VertexiumException("Unhandled element type: " + vertexiumElem.getClass().getName());
                }
            }

            if (parameters.getMinScore() != null) {
                if (parameters.getScoringStrategy() == null) {
                    return false;
                } else {
                    Double elementScore = parameters.getScoringStrategy().getScore(vertexiumElem);
                    if (elementScore == null || elementScore < parameters.getMinScore()) {
                        return false;
                    }
                }
            }
        }

        return !evaluateQueryString
                || vertexiumElem == null
                || !(parameters instanceof QueryStringQueryParameters)
                || ((QueryStringQueryParameters) parameters).getQueryString() == null
                || evaluateQueryString(vertexiumElem, ((QueryStringQueryParameters) parameters).getQueryString());
    }

    private boolean evaluateQueryString(VertexiumObject vertexiumObject, String queryString) {
        if (vertexiumObject instanceof Element) {
            return evaluateQueryString((Element) vertexiumObject, queryString);
        } else if (vertexiumObject instanceof ExtendedDataRow) {
            return evaluateQueryString((ExtendedDataRow) vertexiumObject, queryString);
        } else {
            throw new VertexiumException("Unhandled VertexiumObject type: " + vertexiumObject.getClass().getName());
        }
    }

    private boolean evaluateQueryString(Element element, String queryString) {
        for (Property property : element.getProperties()) {
            if (evaluateQueryStringOnValue(property.getValue(), queryString)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateQueryString(ExtendedDataRow extendedDataRow, String queryString) {
        for (Property property : extendedDataRow.getProperties()) {
            if (evaluateQueryStringOnValue(property.getValue(), queryString)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateQueryStringOnValue(Object value, String queryString) {
        if (value == null) {
            return false;
        }
        if (queryString.equals("*")) {
            return true;
        }
        if (value instanceof StreamingPropertyValue) {
            value = ((StreamingPropertyValue) value).readToString();
        }
        String valueString = value.toString().toLowerCase();
        return valueString.contains(queryString.toLowerCase());
    }

    @Override
    public Stream<T> getHits() {
        List<U> hits  = allHits;
        int skip = Math.max(0, (int) parameters.getSkip());
        if (skip > 0) {
            hits = hits.subList(skip, hits.size());
        }
        if (parameters.getLimit() != null) {
            int limit = Math.min(hits.size(), parameters.getLimit().intValue());
            hits.subList(0, limit);
        }
        return hits.stream().map(transform);
    }

    @Override
    public long getTotalHits() {
        return allHits.size();
    }

    @Override
    public Double getScore(Object id) {
        if (parameters.getScoringStrategy() != null) {
            VertexiumObject vertexiumObject = findVertexiumObjectById(id);
            if (vertexiumObject != null) {
                return parameters.getScoringStrategy().getScore(vertexiumObject);
            }
        }
        return 0.0;
    }

    @Override
    public long getSearchTimeNanoSeconds() {
        return 0;
    }

    private VertexiumObject findVertexiumObjectById(Object id) {
        return allHits.stream()
                .filter(obj -> obj != null && obj.getId().equals(id))
                .map(obj -> (VertexiumObject) obj)
                .findFirst().orElse(null);
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        for (Aggregation agg : this.aggregations) {
            if (agg.getAggregationName().equals(name)) {
                return getAggregationResult(agg, allHits.iterator());
            }
        }
        return null;
    }

    public static boolean isAggregationSupported(Aggregation agg) {
        if (agg instanceof TermsAggregation) {
            return true;
        }
        if (agg instanceof CalendarFieldAggregation) {
            return true;
        }
        if (agg instanceof CardinalityAggregation) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public <TResult extends AggregationResult> TResult getAggregationResult(Aggregation agg, Iterator<U> it) {
        if (agg instanceof TermsAggregation) {
            return (TResult) getTermsAggregationResult((TermsAggregation) agg, it);
        }
        if (agg instanceof CalendarFieldAggregation) {
            return (TResult) getCalendarFieldHistogramResult((CalendarFieldAggregation) agg, it);
        }
        if (agg instanceof CardinalityAggregation) {
            return (TResult) getCardinalityAggregationResult((CardinalityAggregation) agg, it);
        }
        throw new VertexiumException("Unhandled aggregation: " + agg.getClass().getName());
    }

    private CardinalityResult getCardinalityAggregationResult(CardinalityAggregation agg, Iterator<U> it) {
        String fieldName = agg.getPropertyName();

        if (Element.ID_PROPERTY_NAME.equals(fieldName)
                || Edge.LABEL_PROPERTY_NAME.equals(fieldName)
                || Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(fieldName)
                || Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(fieldName)
                || ExtendedDataRow.TABLE_NAME.equals(fieldName)
                || ExtendedDataRow.ROW_ID.equals(fieldName)
                || ExtendedDataRow.ELEMENT_ID.equals(fieldName)
                || ExtendedDataRow.ELEMENT_TYPE.equals(fieldName)) {
            Set<Object> values = new HashSet<>();
            while (it.hasNext()) {
                VertexiumObject vertexiumObject = it.next();
                Iterable<Object> propertyValues = vertexiumObject.getPropertyValues(fieldName);
                for (Object propertyValue : propertyValues) {
                    values.add(propertyValue);
                }
            }
            return new CardinalityResult(values.size());
        } else {
            throw new VertexiumException("Cannot use cardinality aggregation on properties with visibility: " + fieldName);
        }
    }

    private TermsResult getTermsAggregationResult(TermsAggregation agg, Iterator<U> it) {
        String propertyName = agg.getPropertyName();
        Map<Object, List<U>> elementsByProperty = getElementsByProperty(it, propertyName, o -> o);
        elementsByProperty = collapseBucketsByCase(elementsByProperty);

        List<TermsBucket> buckets = new ArrayList<>();
        for (Map.Entry<Object, List<U>> entry : elementsByProperty.entrySet()) {
            Object key = entry.getKey();
            int count = entry.getValue().size();
            Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
            buckets.add(new TermsBucket(key, count, nestedResults));
        }
        return new TermsResult(buckets);
    }

    private Map<Object, List<U>> collapseBucketsByCase(Map<Object, List<U>> elementsByProperty) {
        Map<String, List<Map.Entry<Object, List<U>>>> stringEntries = new HashMap<>();
        Map<Object, List<U>> results = new HashMap<>();

        // for strings first group them by there lowercase version
        for (Map.Entry<Object, List<U>> entry : elementsByProperty.entrySet()) {
            if (entry.getKey() instanceof String) {
                String lowerCaseKey = ((String) entry.getKey()).toLowerCase();
                List<Map.Entry<Object, List<U>>> l = stringEntries.computeIfAbsent(lowerCaseKey, s -> new ArrayList<>());
                l.add(entry);
            } else {
                results.put(entry.getKey(), entry.getValue());
            }
        }

        // for strings find the best key (the one with the most entries) and use that as the bucket name
        for (Map.Entry<String, List<Map.Entry<Object, List<U>>>> entry : stringEntries.entrySet()) {
            results.put(
                    findBestKey(entry.getValue()),
                    entry.getValue().stream()
                            .flatMap(l -> l.getValue().stream())
                            .collect(Collectors.toList())
            );
        }
        return results;
    }

    private Object findBestKey(List<Map.Entry<Object, List<U>>> value) {
        int longestListLength = 0;
        String longestString = null;
        for (Map.Entry<Object, List<U>> entry : value) {
            if (entry.getValue().size() >= longestListLength) {
                longestListLength = entry.getValue().size();
                longestString = (String) entry.getKey();
            }
        }
        return longestString;
    }

    private HistogramResult getCalendarFieldHistogramResult(final CalendarFieldAggregation agg, Iterator<U> it) {
        String propertyName = agg.getPropertyName();
        final Calendar calendar = GregorianCalendar.getInstance(agg.getTimeZone());
        Map<Integer, List<U>> elementsByProperty = getElementsByProperty(it, propertyName, o -> {
            Date d = (Date) o;
            calendar.setTime(d);
            //noinspection MagicConstant
            return calendar.get(agg.getCalendarField());
        });

        Map<Integer, HistogramBucket> buckets = new HashMap<>(24);
        for (Map.Entry<Integer, List<U>> entry : elementsByProperty.entrySet()) {
            int key = entry.getKey();
            int count = entry.getValue().size();
            Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
            buckets.put(key, new HistogramBucket(key, count, nestedResults));
        }
        return new HistogramResult(buckets.values());
    }

    private Map<String, AggregationResult> getNestedResults(Iterable<Aggregation> nestedAggregations, List<U> elements) {
        Map<String, AggregationResult> results = new HashMap<>();
        for (Aggregation nestedAggregation : nestedAggregations) {
            AggregationResult nestedResult = getAggregationResult(nestedAggregation, elements.iterator());
            results.put(nestedAggregation.getAggregationName(), nestedResult);
        }
        return results;
    }

    private <TKey> Map<TKey, List<U>> getElementsByProperty(Iterator<U> it, String propertyName, ValueConverter<TKey> valueConverter) {
        Map<TKey, List<U>> elementsByProperty = new HashMap<>();
        while (it.hasNext()) {
            U vertexiumObject = it.next();
            Iterable<Object> values = vertexiumObject.getPropertyValues(propertyName);
            for (Object value : values) {
                TKey convertedValue = valueConverter.convert(value);
                List<U> list = elementsByProperty.computeIfAbsent(convertedValue, k -> new ArrayList<>());
                list.add(vertexiumObject);
            }
        }
        return elementsByProperty;
    }

    private interface ValueConverter<T> {
        T convert(Object o);
    }
}
