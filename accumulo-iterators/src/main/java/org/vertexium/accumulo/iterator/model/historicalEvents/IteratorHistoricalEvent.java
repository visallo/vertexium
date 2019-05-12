package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class IteratorHistoricalEvent implements Comparable<IteratorHistoricalEvent> {
    private static final byte[] HEADER = {'I', 'H', 'E'};
    protected static final byte TYPE_ID_ADD_EDGE = 0x01;
    protected static final byte TYPE_ID_ADD_EDGE_TO_VERTEX = 0x02;
    protected static final byte TYPE_ID_ADD_PROPERTY = 0x03;
    protected static final byte TYPE_ID_ADD_VERTEX = 0x04;
    protected static final byte TYPE_ID_ALTER_EDGE_LABEL = 0x05;
    protected static final byte TYPE_ID_MARK_HIDDEN = 0x06;
    protected static final byte TYPE_ID_MARK_PROPERTY_HIDDEN = 0x07;
    protected static final byte TYPE_ID_MARK_PROPERTY_VISIBLE = 0x08;
    protected static final byte TYPE_ID_MARK_VISIBLE = 0x09;
    protected static final byte TYPE_ID_SOFT_DELETE_EDGE_TO_VERTEX = 0x0a;
    protected static final byte TYPE_ID_SOFT_DELETE_VERTEX = 0x0b;
    protected static final byte TYPE_ID_SOFT_DELETE_EDGE = 0x0c;
    protected static final byte TYPE_ID_SOFT_DELETE_PROPERTY = 0x0d;
    protected static final byte TYPE_ID_DELETE_VERTEX = 0x0e;
    protected static final byte TYPE_ID_DELETE_EDGE = 0x0f;
    protected static final byte TYPE_ID_ALTER_VERTEX_VISIBILITY = 0x10;
    protected static final byte TYPE_ID_ALTER_EDGE_VISIBILITY = 0x11;
    private final String elementId;
    private final ElementType elementType;
    private final long timestamp;

    public IteratorHistoricalEvent(
        ElementType elementType,
        String elementId,
        long timestamp
    ) {
        this.elementId = elementId;
        this.elementType = elementType;
        this.timestamp = timestamp;
    }

    public static Value encode(List<IteratorHistoricalEvent> events) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        dout.write(HEADER);
        dout.writeInt(events.size());
        for (IteratorHistoricalEvent event : events) {
            event.encode(dout);
        }
        return new Value(out.toByteArray());
    }

    public static List<IteratorHistoricalEvent> decode(Value value, String elementId) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(value.get());
        DataInputStream din = new DataInputStream(in);
        List<IteratorHistoricalEvent> results = new ArrayList<>();
        byte[] header = new byte[HEADER.length];
        int read = din.read(header);
        if (read != header.length) {
            throw new IOException("Invalid header length. Found " + read + " expected " + header.length);
        }
        if (!Arrays.equals(header, HEADER)) {
            throw new IOException("Invalid header.");
        }
        int resultsLength = din.readInt();
        for (int i = 0; i < resultsLength; i++) {
            results.add(decodeEvent(din, elementId));
        }
        if (din.available() > 0) {
            throw new IOException("Expected end of array. Found " + din.available() + " more bytes");
        }
        return results;
    }

    protected void encode(DataOutputStream out) throws IOException {
        out.writeByte(getTypeId());
        DataOutputStreamUtils.encodeElementType(out, getElementType());
        out.writeLong(getTimestamp());
    }

    private static IteratorHistoricalEvent decodeEvent(DataInputStream in, String elementId) throws IOException {
        byte typeId = in.readByte();
        ElementType elementType = DataInputStreamUtils.decodeElementType(in);
        long timestamp = in.readLong();
        switch (typeId) {
            case TYPE_ID_ADD_EDGE:
                return IteratorHistoricalAddEdgeEvent.decode(in, elementId, timestamp);
            case TYPE_ID_ADD_EDGE_TO_VERTEX:
                return IteratorHistoricalAddEdgeToVertexEvent.decode(in, elementId, timestamp);
            case TYPE_ID_ADD_PROPERTY:
                return IteratorHistoricalAddPropertyEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_ADD_VERTEX:
                return IteratorHistoricalAddVertexEvent.decode(in, elementId, timestamp);
            case TYPE_ID_ALTER_EDGE_LABEL:
                return IteratorHistoricalAlterEdgeLabelEvent.decode(in, elementId, timestamp);
            case TYPE_ID_MARK_HIDDEN:
                return IteratorHistoricalMarkHiddenEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_MARK_PROPERTY_HIDDEN:
                return IteratorHistoricalMarkPropertyHiddenEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_MARK_PROPERTY_VISIBLE:
                return IteratorHistoricalMarkPropertyVisibleEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_MARK_VISIBLE:
                return IteratorHistoricalMarkVisibleEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_SOFT_DELETE_EDGE_TO_VERTEX:
                return IteratorHistoricalSoftDeleteEdgeToVertexEvent.decode(in, elementId, timestamp);
            case TYPE_ID_SOFT_DELETE_VERTEX:
                return IteratorHistoricalSoftDeleteVertexEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_SOFT_DELETE_EDGE:
                return IteratorHistoricalSoftDeleteEdgeEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_SOFT_DELETE_PROPERTY:
                return IteratorHistoricalSoftDeletePropertyEvent.decode(in, elementType, elementId, timestamp);
            case TYPE_ID_DELETE_VERTEX:
                return IteratorHistoricalDeleteVertexEvent.decode(in, elementId, timestamp);
            case TYPE_ID_DELETE_EDGE:
                return IteratorHistoricalDeleteEdgeEvent.decode(in, elementId, timestamp);
            case TYPE_ID_ALTER_VERTEX_VISIBILITY:
                return IteratorHistoricalAlterVertexVisibilityEvent.decode(in, elementId, timestamp);
            case TYPE_ID_ALTER_EDGE_VISIBILITY:
                return IteratorHistoricalAlterEdgeVisibilityEvent.decode(in, elementId, timestamp);
            default:
                throw new IOException("Unexpected type: " + typeId);
        }
    }

    protected abstract byte getTypeId();

    public String getElementId() {
        return elementId;
    }

    public ElementType getElementType() {
        return elementType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(IteratorHistoricalEvent otherEvent) {
        return getHistoricalEventId().compareTo(otherEvent.getHistoricalEventId());
    }

    public HistoricalEventId getHistoricalEventId() {
        return new HistoricalEventId(getTimestamp(), getElementType(), getElementId(), getHistoricalEventIdSubOrder());
    }

    protected String getHistoricalEventIdSubOrder() {
        return "~";
    }
}
