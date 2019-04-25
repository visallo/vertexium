package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class MarkPropertyVisibleMutation extends PropertyMutation {
    private final Object data;

    public MarkPropertyVisibleMutation(
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility,
        long timestamp,
        Visibility hiddenVisibility,
        Object data
    ) {
        super(timestamp, propertyKey, propertyName, propertyVisibility, hiddenVisibility);
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
