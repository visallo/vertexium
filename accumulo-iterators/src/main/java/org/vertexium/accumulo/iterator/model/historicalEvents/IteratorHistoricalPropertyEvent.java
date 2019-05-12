package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class IteratorHistoricalPropertyEvent extends IteratorHistoricalEvent {
    private final ByteSequence propertyKey;
    private final ByteSequence propertyName;
    private final ByteSequence propertyVisibilityString;

    public IteratorHistoricalPropertyEvent(
        ElementType elementType,
        String elementId,
        ByteSequence propertyKey,
        ByteSequence propertyName,
        ByteSequence propertyVisibilityString,
        long timestamp
    ) {
        super(elementType, elementId, timestamp);
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibilityString = propertyVisibilityString;
    }

    public ByteSequence getPropertyKey() {
        return propertyKey;
    }

    public ByteSequence getPropertyName() {
        return propertyName;
    }

    public ByteSequence getPropertyVisibilityString() {
        return propertyVisibilityString;
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getPropertyKey());
        DataOutputStreamUtils.encodeByteSequence(out, getPropertyName());
        DataOutputStreamUtils.encodeByteSequence(out, getPropertyVisibilityString());
    }

    @Override
    protected String getHistoricalEventIdSubOrder() {
        return "prop:" + getPropertyName();
    }
}
