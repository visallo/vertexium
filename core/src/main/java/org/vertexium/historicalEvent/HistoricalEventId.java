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
    private final int subOrder;

    // for Serializable
    protected HistoricalEventId() {
        timestamp = null;
        elementType = null;
        elementId = null;
        subOrder = 0;
    }

    public HistoricalEventId(
        ZonedDateTime timestamp,
        ElementType elementType,
        String elementId,
        int subOrder
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
            Integer.parseInt(parts[3], 16)
        );
    }

    @Override
    public String toString() {
        return encoder.encode(
            MultiFieldStringEncoder.timestampToString(getTimestamp()),
            getElementType().name(),
            getElementId(),
            String.format("%04x", getSubOrder())
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

        return Integer.compare(getSubOrder(), other.getSubOrder());
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

    public int getSubOrder() {
        return subOrder;
    }
}
