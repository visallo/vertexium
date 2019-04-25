package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalMarkPropertyVisibleEvent extends HistoricalPropertyEvent {
    private final Visibility hiddenVisibility;
    private final Object data;

    public HistoricalMarkPropertyVisibleEvent(
        ElementType elementType,
        String id,
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility,
        Visibility hiddenVisibility,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, id, propertyKey, propertyName, propertyVisibility, timestamp, fetchHints);
        this.hiddenVisibility = hiddenVisibility;
        this.data = data;
    }

    public Visibility getHiddenVisibility() {
        return hiddenVisibility;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format("%s, hiddenVisibility=%s, data=%s}", super.toString(), getHiddenVisibility(), getData());
    }
}
