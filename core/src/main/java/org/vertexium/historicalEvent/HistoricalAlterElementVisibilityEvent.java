package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public abstract class HistoricalAlterElementVisibilityEvent extends HistoricalEvent {
    private final Visibility oldVisibility;
    private final Visibility newVisibility;
    private final Object data;

    public HistoricalAlterElementVisibilityEvent(
        ElementType elementType,
        String elementId,
        Visibility oldVisibility,
        Visibility newVisibility,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, elementId, timestamp, fetchHints);
        this.oldVisibility = oldVisibility;
        this.newVisibility = newVisibility;
        this.data = data;
    }

    public Visibility getOldVisibility() {
        return oldVisibility;
    }

    public Visibility getNewVisibility() {
        return newVisibility;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format(
            "%s, oldVisibility='%s', newVisibility='%s', data=%s",
            super.toString(),
            getOldVisibility(),
            getNewVisibility(),
            getData()
        );
    }
}
