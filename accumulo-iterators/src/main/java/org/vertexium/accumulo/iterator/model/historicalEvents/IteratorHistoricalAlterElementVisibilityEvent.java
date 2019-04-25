package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;

public abstract class IteratorHistoricalAlterElementVisibilityEvent extends IteratorHistoricalEvent {
    private final ByteSequence oldVisibility;
    private final ByteSequence newVisibility;
    private final Value data;

    public IteratorHistoricalAlterElementVisibilityEvent(
        ElementType elementType,
        String elementId,
        ByteSequence oldVisibility,
        ByteSequence newVisibility,
        long timestamp,
        Value data
    ) {
        super(elementType, elementId, timestamp);
        this.oldVisibility = oldVisibility;
        this.newVisibility = newVisibility;
        this.data = data;
    }

    public ByteSequence getOldVisibility() {
        return oldVisibility;
    }

    public ByteSequence getNewVisibility() {
        return newVisibility;
    }

    public Value getData() {
        return data;
    }
}
