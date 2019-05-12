package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.vertexium.accumulo.iterator.model.ElementType;

public abstract class IteratorHistoricalAddElementEvent extends IteratorHistoricalEvent {
    public IteratorHistoricalAddElementEvent(ElementType type, String elementId, Long timestamp) {
        super(type, elementId, timestamp);
    }

    @Override
    protected String getHistoricalEventIdSubOrder() {
        return "!";
    }
}
