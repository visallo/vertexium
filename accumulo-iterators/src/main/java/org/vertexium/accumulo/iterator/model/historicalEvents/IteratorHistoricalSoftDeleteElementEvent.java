package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;

public abstract class IteratorHistoricalSoftDeleteElementEvent extends IteratorHistoricalEvent {
    private final Value data;

    public IteratorHistoricalSoftDeleteElementEvent(
        ElementType elementType,
        String elementId,
        long timestamp,
        Value data
    ) {
        super(elementType, elementId, timestamp);
        this.data = data;
    }

    public Value getData() {
        return data;
    }
}
