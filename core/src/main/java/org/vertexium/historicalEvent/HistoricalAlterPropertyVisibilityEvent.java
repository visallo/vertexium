package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.HistoricalEventsFetchHints;
import org.vertexium.Visibility;

import java.time.ZonedDateTime;

public class HistoricalAlterPropertyVisibilityEvent extends HistoricalPropertyEvent {
    private final Object data;
    private final Visibility oldPropertyVisibility;

    public HistoricalAlterPropertyVisibilityEvent(
        ElementType elementType,
        String id,
        String propertyKey,
        String propertyName,
        Visibility oldPropertyVisibility,
        Visibility newPropertyVisibility,
        ZonedDateTime timestamp,
        Object data,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, id, propertyKey, propertyName, newPropertyVisibility, timestamp, fetchHints);
        this.oldPropertyVisibility = oldPropertyVisibility;
        this.data = data;
    }

    public Visibility getOldPropertyVisibility() {
        return oldPropertyVisibility;
    }

    public Visibility getNewPropertyVisibility() {
        return getPropertyVisibility();
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
