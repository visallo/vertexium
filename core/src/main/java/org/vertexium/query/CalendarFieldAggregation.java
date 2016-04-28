package org.vertexium.query;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class CalendarFieldAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    public static final Class<? extends HistogramResult> RESULT_CLASS = HistogramResult.class;
    private final String aggregationName;
    private final String propertyName;
    private final Long minDocumentCount;
    private final List<Aggregation> nestedAggregations = new ArrayList<>();
    private final TimeZone timeZone;
    private final int calendarField;

    public CalendarFieldAggregation(
            String aggregationName,
            String propertyName,
            Long minDocumentCount,
            TimeZone timeZone,
            int calendarField
    ) {
        this.aggregationName = aggregationName;
        this.propertyName = propertyName;
        this.minDocumentCount = minDocumentCount;
        this.timeZone = timeZone;
        this.calendarField = calendarField;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getPropertyName() {
        return propertyName;
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

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public int getCalendarField() {
        return calendarField;
    }
}
