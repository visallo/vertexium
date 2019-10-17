package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalMarkPropertyVisibleEvent extends IteratorHistoricalPropertyEvent {
    private final ByteSequence hiddenVisibility;
    private final Value data;

    public IteratorHistoricalMarkPropertyVisibleEvent(
        ElementType elementType,
        String elementId,
        ByteSequence propertyKey,
        ByteSequence propertyName,
        ByteSequence propertyVisibilityString,
        ByteSequence hiddenVisibility,
        long timestamp,
        Value data
    ) {
        super(elementType, elementId, propertyKey, propertyName, propertyVisibilityString, timestamp);
        this.hiddenVisibility = hiddenVisibility;
        this.data = data;
    }

    public ByteSequence getHiddenVisibility() {
        return hiddenVisibility;
    }

    public Value getData() {
        return data;
    }

    static IteratorHistoricalEvent decode(DataInputStream in, ElementType elementType, String elementId, long timestamp) throws IOException {
        ByteSequence propertyKey = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence propertyName = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence propertyVisibility = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence hiddenVisibility = DataInputStreamUtils.decodeByteSequence(in);
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalMarkPropertyVisibleEvent(
            elementType,
            elementId,
            propertyKey,
            propertyName,
            propertyVisibility,
            hiddenVisibility,
            timestamp,
            data
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getHiddenVisibility());
        DataOutputStreamUtils.encodeValue(out, getData());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_MARK_PROPERTY_VISIBLE;
    }
}
