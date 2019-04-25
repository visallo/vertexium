package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.vertexium.accumulo.iterator.model.SortDirection;

public class IteratorHistoricalEventsFetchHints {
    private final Long startTime;
    private final Long endTime;
    private final SortDirection sortDirection;
    private final Long limit;
    private final boolean includePreviousPropertyValues;
    private final boolean includePropertyValues;

    public IteratorHistoricalEventsFetchHints(
        Long startTime,
        Long endTime,
        SortDirection sortDirection,
        Long limit,
        boolean includePreviousPropertyValues,
        boolean includePropertyValues
    ) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.sortDirection = sortDirection;
        this.limit = limit;
        this.includePreviousPropertyValues = includePreviousPropertyValues;
        this.includePropertyValues = includePropertyValues;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public SortDirection getSortDirection() {
        return sortDirection;
    }

    public Long getLimit() {
        return limit;
    }

    public boolean isIncludePreviousPropertyValues() {
        return includePreviousPropertyValues;
    }

    public boolean isIncludePropertyValues() {
        return includePropertyValues;
    }
}
