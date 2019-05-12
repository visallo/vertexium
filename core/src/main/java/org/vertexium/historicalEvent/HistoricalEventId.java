package org.vertexium.historicalEvent;

import org.vertexium.ElementType;
import org.vertexium.util.MultiFieldStringEncoder;

import java.io.Serializable;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class HistoricalEventId implements Serializable, Comparable<HistoricalEventId> {
    private static final String SEPARATOR = "&";
    private static final MultiFieldStringEncoder encoder = new MultiFieldStringEncoder(SEPARATOR, 4);
    private final ZonedDateTime timestamp;
    private final ElementType elementType;
    private final String elementId;
    private final String subOrder;

    // for Serializable
    protected HistoricalEventId() {
        timestamp = null;
        elementType = null;
        elementId = null;
        subOrder = null;
    }

    public HistoricalEventId(
        ZonedDateTime timestamp,
        ElementType elementType,
        String elementId,
        String subOrder
    ) {
        this.timestamp = timestamp;
        this.elementType = elementType;
        this.elementId = elementId;
        this.subOrder = subOrder;
    }

    public static HistoricalEventId fromString(String str) {
        String[] parts = encoder.decode(str);
        return new HistoricalEventId(
            MultiFieldStringEncoder.timestampFromString(parts[0], ZoneOffset.UTC),
            ElementType.valueOf(parts[1]),
            parts[2],
            parts[3]
        );
    }

    @Override
    public String toString() {
        return encoder.encode(
            MultiFieldStringEncoder.timestampToString(getTimestamp()),
            getElementType().name(),
            getElementId(),
            getSubOrder()
        );
    }

    @Override
    public int compareTo(HistoricalEventId other) {
        int result = getTimestamp().compareTo(other.getTimestamp());
        if (result != 0) {
            return result;
        }

        result = getElementType().compareTo(other.getElementType());
        if (result != 0) {
            return result;
        }

        result = getElementId().compareTo(other.getElementId());
        if (result != 0) {
            return result;
        }

        return getSubOrder().compareTo(other.getSubOrder());
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public ElementType getElementType() {
        return elementType;
    }

    public String getElementId() {
        return elementId;
    }

    public String getSubOrder() {
        return subOrder;
    }
}
