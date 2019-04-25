package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalSoftDeletePropertyEvent extends HistoricalPropertyEvent {
    private final Object data;

    public HistoricalSoftDeletePropertyEvent(
        ElementType elementType,
        String id,
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, id, propertyKey, propertyName, propertyVisibility, timestamp, fetchHints);
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format(
            "%s, data=%s}",
            super.toString(),
            getData()
        );
    }
}
