package org.vertexium.mutation;

import org.vertexium.Visibility;

public class MarkPropertyVisibleMutation {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;
    private final Long timestamp;
    private final Visibility visibility;
    private final Object eventData;

    public MarkPropertyVisibleMutation(
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object eventData
    ) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
        this.timestamp = timestamp;
        this.visibility = visibility;
        this.eventData = eventData;
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

    public Long getTimestamp() {
        return timestamp;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    public Object getEventData() {
        return eventData;
    }
}
