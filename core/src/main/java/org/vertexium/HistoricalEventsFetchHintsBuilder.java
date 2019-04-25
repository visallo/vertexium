package org.vertexium;

import java.time.ZonedDateTime;

public class HistoricalEventsFetchHintsBuilder {
    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    private HistoricalEventsFetchHints.SortDirection sortDirection = HistoricalEventsFetchHints.SortDirection.ASCENDING;
    private Long limit;
    private boolean includePreviousPropertyValues = true;
    private boolean includePropertyValues = true;

    public HistoricalEventsFetchHints build() {
        return new HistoricalEventsFetchHints(
            startTime,
            endTime,
            sortDirection,
            limit,
            includePreviousPropertyValues,
            includePropertyValues
        );
    }

    public HistoricalEventsFetchHintsBuilder startTime(ZonedDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    public HistoricalEventsFetchHintsBuilder endTime(ZonedDateTime endTime) {
        this.endTime = endTime;
        return this;
    }

    public HistoricalEventsFetchHintsBuilder sortDirection(HistoricalEventsFetchHints.SortDirection sortDirection) {
        this.sortDirection = sortDirection;
        return this;
    }

    public HistoricalEventsFetchHintsBuilder limit(Long limit) {
        this.limit = limit;
        return this;
    }

    public HistoricalEventsFetchHintsBuilder includePreviousPropertyValues(boolean includePreviousPropertyValues) {
        this.includePreviousPropertyValues = includePreviousPropertyValues;
        return this;
    }

    public HistoricalEventsFetchHintsBuilder includePropertyValues(boolean includePropertyValues) {
        this.includePropertyValues = includePropertyValues;
        return this;
    }
}
