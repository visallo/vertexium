package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public abstract class HistoricalPropertyEvent extends HistoricalEvent {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;

    public HistoricalPropertyEvent(
        ElementType elementType,
        String elementId,
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility,
        ZonedDateTime timestamp,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, elementId, timestamp, fetchHints);
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    @Override
    public String toString() {
        return String.format(
            "%s, propertyKey='%s', propertyName='%s', propertyVisibility=%s",
            super.toString(),
            getPropertyKey(),
            getPropertyName(),
            getPropertyVisibility()
        );
    }

    @Override
    protected String getHistoricalEventIdSubOrder() {
        return "prop:" + getPropertyName();
    }
}
