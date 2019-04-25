package org.vertexium;

import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;

import java.time.ZonedDateTime;
import java.util.stream.Stream;

public class HistoricalEventsFetchHints {
    private final ZonedDateTime startTime;
    private final ZonedDateTime endTime;
    private final SortDirection sortDirection;
    private final Long limit;
    private final boolean includePreviousPropertyValues;
    private final boolean includePropertyValues;

    public static final HistoricalEventsFetchHints ALL = new HistoricalEventsFetchHintsBuilder().build();

    HistoricalEventsFetchHints(
        ZonedDateTime startTime,
        ZonedDateTime endTime,
        SortDirection sortDirection,
        Long limit,
        boolean includePreviousPropertyValues,
        boolean includePropertyValues
    ) {
        if (includePreviousPropertyValues && !includePropertyValues) {
            throw new VertexiumException("Cannot include previous property values without also including property values");
        }
        if (startTime != null && endTime != null && startTime.compareTo(endTime) > 0) {
            throw new VertexiumException("Start time cannot be after end time");
        }
        this.startTime = startTime;
        this.endTime = endTime;
        this.sortDirection = sortDirection;
        this.limit = limit;
        this.includePreviousPropertyValues = includePreviousPropertyValues;
        this.includePropertyValues = includePropertyValues;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public ZonedDateTime getEndTime() {
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

    public Stream<HistoricalEvent> applyToResults(Stream<HistoricalEvent> events, HistoricalEventId after) {
        switch (getSortDirection()) {
            case ASCENDING:
                events = events.sorted();
                break;
            case DESCENDING:
                events = events.sorted((o1, o2) -> -o1.compareTo(o2));
                break;
            default:
                throw new VertexiumException("Unhandled sort direction: " + getSortDirection());
        }

        if (startTime != null || endTime != null) {
            long startTimeMillis = startTime == null ? 0 : startTime.toInstant().toEpochMilli();
            long endTimeMillis = endTime == null ? Long.MAX_VALUE : endTime.toInstant().toEpochMilli();
            events = events.filter(event -> {
                long ts = event.getTimestamp().toInstant().toEpochMilli();
                if (ts < startTimeMillis) {
                    return false;
                }
                if (ts > endTimeMillis) {
                    return false;
                }
                return true;
            });
        }

        if (after != null) {
            events = events.filter(event -> {
                int i = event.getHistoricalEventId().compareTo(after);
                switch (getSortDirection()) {
                    case ASCENDING:
                        return i > 0;
                    case DESCENDING:
                        return i < 0;
                    default:
                        throw new VertexiumException("Unhandled sort direction: " + getSortDirection());
                }
            });
        }

        if (limit != null) {
            events = events.limit(limit);
        }

        return events;
    }

    @Override
    public String toString() {
        return String.format(
            "HistoricalEventsFetchHints{startTime=%s, endTime=%s, sortDirection=%s, limit=%d, includePreviousPropertyValues=%s, includePropertyValues=%s}",
            startTime,
            endTime,
            sortDirection,
            limit,
            includePreviousPropertyValues,
            includePropertyValues
        );
    }

    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }
}
