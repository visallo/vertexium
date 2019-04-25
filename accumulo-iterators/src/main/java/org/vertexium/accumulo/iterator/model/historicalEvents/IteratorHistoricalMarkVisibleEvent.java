package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalMarkVisibleEvent extends IteratorHistoricalEvent {
    private final ByteSequence hiddenVisibility;
    private final Value data;

    public IteratorHistoricalMarkVisibleEvent(
        ElementType elementType,
        String elementId,
        ByteSequence hiddenVisibility,
        long timestamp,
        Value data
    ) {
        super(elementType, elementId, timestamp);
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
        ByteSequence hiddenVisibility = DataInputStreamUtils.decodeByteSequence(in);
        Value data = DataInputStreamUtils.decodeValue(in);
        return new IteratorHistoricalMarkVisibleEvent(
            elementType,
            elementId,
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
        return TYPE_ID_MARK_VISIBLE;
    }
}
