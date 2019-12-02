package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.model.proto.HistoricalEvent;
import org.vertexium.accumulo.iterator.model.proto.HistoricalEvents;
import org.vertexium.accumulo.iterator.model.proto.HistoricalEventsItem;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class IteratorHistoricalEvent implements Comparable<IteratorHistoricalEvent> {
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
        HistoricalEvents.Builder results = HistoricalEvents.newBuilder();
        for (IteratorHistoricalEvent event : events) {
            results.addEvents(event.encode());
        }
        return new Value(results.build().toByteArray());
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

    protected abstract HistoricalEventsItem encode();

    protected HistoricalEvent encodeEvent() {
        return HistoricalEvent.newBuilder()
            .setElementType(getElementType() == ElementType.VERTEX
                ? org.vertexium.accumulo.iterator.model.proto.ElementType.VERTEX
                : org.vertexium.accumulo.iterator.model.proto.ElementType.EDGE
            )
            .setTimestamp(getTimestamp())
            .build();
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

    protected int getHistoricalEventIdSubOrder() {
        return 1000;
    }
}
