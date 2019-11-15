package org.vertexium.query;

import org.vertexium.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.vertexium.query.TermsResult.NOT_COMPUTED;

public class DefaultGraphQueryIterableWithAggregations<T extends VertexiumObject> extends DefaultGraphQueryIterable<T> {
    private final Collection<Aggregation> aggregations;

    public DefaultGraphQueryIterableWithAggregations(
        QueryParameters parameters,
        Iterable<T> iterable,
        boolean evaluateQueryString,
        boolean evaluateHasContainers,
        boolean evaluateSortContainers,
        Collection<Aggregation> aggregations
    ) {
        super(parameters, iterable, evaluateQueryString, evaluateHasContainers, evaluateSortContainers);
        this.aggregations = aggregations;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        for (Aggregation agg : this.aggregations) {
            if (agg.getAggregationName().equals(name)) {
                return getAggregationResult(agg, this.iterator(true));
            }
        }
        return super.getAggregationResult(name, resultType);
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
    public <TResult extends AggregationResult> TResult getAggregationResult(Aggregation agg, Iterator<T> it) {
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

    private CardinalityResult getCardinalityAggregationResult(CardinalityAggregation agg, Iterator<T> it) {
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
                T vertexiumObject = it.next();
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

    private TermsResult getTermsAggregationResult(TermsAggregation agg, Iterator<T> it) {
        String propertyName = agg.getPropertyName();
        String missingFlag = "PROPERTY_MISSING";
        Map<Object, List<T>> elementsByProperty = getElementsByProperty(it, propertyName, o -> o, missingFlag);
        elementsByProperty = collapseBucketsByCase(elementsByProperty);

        long hasNotCount = NOT_COMPUTED;
        if (agg.isIncludeHasNotCount()) {
            hasNotCount = elementsByProperty.containsKey(missingFlag) ? elementsByProperty.get(missingFlag).size() : 0;
        }
        elementsByProperty.remove(missingFlag);

        long other = 0;
        List<TermsBucket> buckets = new ArrayList<>();
        for (Map.Entry<Object, List<T>> entry : elementsByProperty.entrySet()) {
            Object key = entry.getKey();
            int count = entry.getValue().size();
            if (agg.getSize() == null || buckets.size() < agg.getSize()) {
                Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
                buckets.add(new TermsBucket(key, count, nestedResults));
            } else {
                other += count;
            }
        }
        return new TermsResult(buckets, other, 0, hasNotCount);
    }

    private Map<Object, List<T>> collapseBucketsByCase(Map<Object, List<T>> elementsByProperty) {
        Map<String, List<Map.Entry<Object, List<T>>>> stringEntries = new HashMap<>();
        Map<Object, List<T>> results = new HashMap<>();

        // for strings first group them by there lowercase version
        for (Map.Entry<Object, List<T>> entry : elementsByProperty.entrySet()) {
            if (entry.getKey() instanceof String) {
                String lowerCaseKey = ((String) entry.getKey()).toLowerCase();
                List<Map.Entry<Object, List<T>>> l = stringEntries.computeIfAbsent(lowerCaseKey, s -> new ArrayList<>());
                l.add(entry);
            } else {
                results.put(entry.getKey(), entry.getValue());
            }
        }

        // for strings find the best key (the one with the most entries) and use that as the bucket name
        for (Map.Entry<String, List<Map.Entry<Object, List<T>>>> entry : stringEntries.entrySet()) {
            results.put(
                findBestKey(entry.getValue()),
                entry.getValue().stream()
                    .flatMap(l -> l.getValue().stream())
                    .collect(Collectors.toList())
            );
        }
        return results;
    }

    private Object findBestKey(List<Map.Entry<Object, List<T>>> value) {
        int longestListLength = 0;
        String longestString = null;
        for (Map.Entry<Object, List<T>> entry : value) {
            if (entry.getValue().size() >= longestListLength) {
                longestListLength = entry.getValue().size();
                longestString = (String) entry.getKey();
            }
        }
        return longestString;
    }

    private HistogramResult getCalendarFieldHistogramResult(final CalendarFieldAggregation agg, Iterator<T> it) {
        String propertyName = agg.getPropertyName();
        final Calendar calendar = GregorianCalendar.getInstance(agg.getTimeZone());
        Map<Integer, List<T>> elementsByProperty = getElementsByProperty(it, propertyName, o -> {
            Date d = (Date) o;
            calendar.setTime(d);
            //noinspection MagicConstant
            return calendar.get(agg.getCalendarField());
        }, null);

        Map<Integer, HistogramBucket> buckets = new HashMap<>(24);
        for (Map.Entry<Integer, List<T>> entry : elementsByProperty.entrySet()) {
            int key = entry.getKey();
            int count = entry.getValue().size();
            Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
            buckets.put(key, new HistogramBucket(key, count, nestedResults));
        }
        return new HistogramResult(buckets.values());
    }

    private Map<String, AggregationResult> getNestedResults(Iterable<Aggregation> nestedAggregations, List<T> elements) {
        Map<String, AggregationResult> results = new HashMap<>();
        for (Aggregation nestedAggregation : nestedAggregations) {
            AggregationResult nestedResult = getAggregationResult(nestedAggregation, elements.iterator());
            results.put(nestedAggregation.getAggregationName(), nestedResult);
        }
        return results;
    }

    private <TKey> Map<TKey, List<T>> getElementsByProperty(Iterator<T> it, String propertyName, ValueConverter<TKey> valueConverter, TKey missingFlag) {
        Map<TKey, List<T>> elementsByProperty = new HashMap<>();
        while (it.hasNext()) {
            T vertexiumObject = it.next();
            Iterable<Object> values = vertexiumObject.getPropertyValues(propertyName);
            boolean hasValues = false;
            for (Object value : values) {
                hasValues = true;
                TKey convertedValue = valueConverter.convert(value);
                elementsByProperty.computeIfAbsent(convertedValue, k -> new ArrayList<>())
                    .add(vertexiumObject);
            }

            if (!hasValues && missingFlag != null) {
                elementsByProperty.computeIfAbsent(missingFlag, k -> new ArrayList<>()).add(vertexiumObject);
            }
        }
        return elementsByProperty;
    }

    private interface ValueConverter<T> {
        T convert(Object o);
    }
}
