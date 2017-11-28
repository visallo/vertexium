package org.vertexium.query;

import org.vertexium.VertexiumException;
import org.vertexium.VertexiumObject;

import java.util.*;

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
        throw new VertexiumException("Unhandled aggregation: " + agg.getClass().getName());
    }

    private TermsResult getTermsAggregationResult(TermsAggregation agg, Iterator<T> it) {
        String propertyName = agg.getPropertyName();
        Map<Object, List<T>> elementsByProperty = getElementsByProperty(it, propertyName, new IdentityValueConverter());

        List<TermsBucket> buckets = new ArrayList<>();
        for (Map.Entry<Object, List<T>> entry : elementsByProperty.entrySet()) {
            Object key = entry.getKey();
            int count = entry.getValue().size();
            Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
            buckets.add(new TermsBucket(key, count, nestedResults));
        }
        return new TermsResult(buckets);
    }

    private HistogramResult getCalendarFieldHistogramResult(final CalendarFieldAggregation agg, Iterator<T> it) {
        String propertyName = agg.getPropertyName();
        final Calendar calendar = GregorianCalendar.getInstance(agg.getTimeZone());
        Map<Integer, List<T>> elementsByProperty = getElementsByProperty(it, propertyName, new ValueConverter<Integer>() {
            @Override
            public Integer convert(Object o) {
                Date d = (Date) o;
                calendar.setTime(d);
                //noinspection MagicConstant
                return calendar.get(agg.getCalendarField());
            }
        });

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

    private <TKey> Map<TKey, List<T>> getElementsByProperty(Iterator<T> it, String propertyName, ValueConverter<TKey> valueConverter) {
        Map<TKey, List<T>> elementsByProperty = new HashMap<>();
        while (it.hasNext()) {
            T vertexiumObject = it.next();
            Iterable<Object> values = vertexiumObject.getPropertyValues(propertyName);
            for (Object value : values) {
                TKey convertedValue = valueConverter.convert(value);
                elementsByProperty.computeIfAbsent(convertedValue, k -> new ArrayList<>())
                        .add(vertexiumObject);
            }
        }
        return elementsByProperty;
    }

    private interface ValueConverter<T> {
        T convert(Object o);
    }

    private class IdentityValueConverter implements ValueConverter<Object> {
        @Override
        public Object convert(Object o) {
            return o;
        }
    }
}
