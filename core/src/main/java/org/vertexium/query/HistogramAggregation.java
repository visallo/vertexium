package org.vertexium.query;

import org.vertexium.VertexiumException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HistogramAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    private final String aggregationName;
    private final String fieldName;
    private final String interval;
    private final Long minDocumentCount;
    private final List<Aggregation> nestedAggregations = new ArrayList<>();
    private ExtendedBounds<?> extendedBounds;

    public HistogramAggregation(String aggregationName, String fieldName, String interval, Long minDocumentCount) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
        this.interval = interval;
        this.minDocumentCount = minDocumentCount;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getInterval() {
        return interval;
    }

    public Long getMinDocumentCount() {
        return minDocumentCount;
    }

    @Override
    public void addNestedAggregation(Aggregation nestedAggregation) {
        this.nestedAggregations.add(nestedAggregation);
    }

    @Override
    public Iterable<Aggregation> getNestedAggregations() {
        return nestedAggregations;
    }

    public ExtendedBounds<?> getExtendedBounds() {
        return extendedBounds;
    }

    public void setExtendedBounds(ExtendedBounds<?> extendedBounds) {
        this.extendedBounds = extendedBounds;
    }

    public static class ExtendedBounds<T> implements Serializable {
        private static final long serialVersionUID = 6441762717687378245L;
        private final T min;
        private final T max;

        public ExtendedBounds(T min, T max) {
            if (min == null && max == null) {
                throw new VertexiumException("Either min or max needs to not be null");
            }
            this.min = min;
            this.max = max;
        }

        public T getMin() {
            return min;
        }

        public T getMax() {
            return max;
        }

        @SuppressWarnings("unchecked")
        public Class<? extends T> getMinMaxType() {
            if (min != null) {
                return (Class<? extends T>) min.getClass();
            }
            if (max != null) {
                return (Class<? extends T>) max.getClass();
            }
            throw new VertexiumException("Invalid state. min or max must not be null.");
        }
    }
}
