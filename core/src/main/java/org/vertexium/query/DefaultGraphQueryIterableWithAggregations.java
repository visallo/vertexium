package org.vertexium.query;

import org.vertexium.Element;
import org.vertexium.VertexiumException;

import java.util.*;

public class DefaultGraphQueryIterableWithAggregations<T extends Element> extends DefaultGraphQueryIterable<T> {
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
        return false;
    }

    public <TResult extends AggregationResult> TResult getAggregationResult(Aggregation agg, Iterator<T> it) {
        if (agg instanceof TermsAggregation) {
            return getTermsAggregationResult((TermsAggregation) agg, it);
        }
        throw new VertexiumException("Unhandled aggregation: " + agg.getClass().getName());
    }

    private <TResult extends AggregationResult> TResult getTermsAggregationResult(TermsAggregation agg, Iterator<T> it) {
        String propertyName = agg.getPropertyName();
        Map<Object, List<T>> elementsByProperty = getElementsByProperty(it, propertyName);

        List<TermsBucket> buckets = new ArrayList<>();
        for (Map.Entry<Object, List<T>> entry : elementsByProperty.entrySet()) {
            Object key = entry.getKey();
            int count = entry.getValue().size();
            Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
            buckets.add(new TermsBucket(key, count, nestedResults));
        }
        return (TResult) new TermsResult(buckets);
    }

    private Map<String, AggregationResult> getNestedResults(Iterable<Aggregation> nestedAggregations, List<T> elements) {
        Map<String, AggregationResult> results = new HashMap<>();
        for (Aggregation nestedAggregation : nestedAggregations) {
            AggregationResult nestedResult = getAggregationResult(nestedAggregation, elements.iterator());
            results.put(nestedAggregation.getAggregationName(), nestedResult);
        }
        return results;
    }

    private Map<Object, List<T>> getElementsByProperty(Iterator<T> it, String propertyName) {
        Map<Object, List<T>> elementsByProperty = new HashMap<>();
        while (it.hasNext()) {
            T elem = it.next();
            Iterable<Object> values = elem.getPropertyValues(propertyName);
            for (Object value : values) {
                List<T> list = elementsByProperty.get(value);
                if (list == null) {
                    list = new ArrayList<T>();
                    elementsByProperty.put(value, list);
                }
                list.add(elem);
            }
        }
        return elementsByProperty;
    }
}
