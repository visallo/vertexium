package org.vertexium.query;

import java.util.ArrayList;
import java.util.List;

public class RangeAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    private final String aggregationName;
    private final String fieldName;
    private String format;

    private List<Range> ranges = new ArrayList<>();

    private final List<Aggregation> nestedAggregations = new ArrayList<>();

    public RangeAggregation(String aggregationName, String fieldName) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
    }

    public RangeAggregation(String aggregationName, String fieldName, String format) {
        this(aggregationName, fieldName);
        this.format = format;
    }

    @Override
    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFormat() {
        return format;
    }

    @Override
    public void addNestedAggregation(Aggregation nestedAggregation) {
        this.nestedAggregations.add(nestedAggregation);
    }

    @Override
    public Iterable<Aggregation> getNestedAggregations() {
        return nestedAggregations;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public void addRange(Object from, Object to) {
        addRange(null, from, to);
    }

    public void addRange(String key, Object from, Object to) {
        ranges.add(new Range(key, from, to));
    }

    public void addUnboundedTo(Object to) {
        addRange(null, null, to);
    }

    public void addUnboundedTo(String key, Object to) {
        addRange(key, null, to);
    }

    public void addUnboundedFrom(Object from) {
        addRange(null, from, null);
    }

    public void addUnboundedFrom(String key, Object from) {
        addRange(key, from, null);
    }

    public class Range {
        private String key;
        private Object from;
        private Object to;

        public Range(String key, Object from, Object to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        public String getKey() {
            return key;
        }

        public Object getFrom() {
            return from;
        }

        public Object getTo() {
            return to;
        }
    }
}
