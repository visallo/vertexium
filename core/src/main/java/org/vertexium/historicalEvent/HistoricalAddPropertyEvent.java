package org.vertexium.historicalEvent;

import org.vertexium.*;

import java.time.ZonedDateTime;

public class HistoricalAddPropertyEvent extends HistoricalPropertyEvent {
    private final Object value;
    private final Object previousValue;
    private final Metadata metadata;

    public HistoricalAddPropertyEvent(
        ElementType elementType,
        String elementId,
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility,
        Object previousValue,
        Object value,
        Metadata metadata,
        ZonedDateTime timestamp,
        HistoricalEventsFetchHints fetchHints
    ) {
        super(elementType, elementId, propertyKey, propertyName, propertyVisibility, timestamp, fetchHints);
        if (!fetchHints.isIncludePropertyValues() && value != null) {
            throw new VertexiumException("value should be null when property values are not included in fetch hints");
        }
        this.value = value;
        if (!fetchHints.isIncludePreviousPropertyValues() && previousValue != null) {
            throw new VertexiumException("previousValue should be null when previous property values are not included in fetch hints");
        }
        this.previousValue = previousValue;
        this.metadata = metadata;
    }

    public Object getPreviousValue() {
        if (!getFetchHints().isIncludePreviousPropertyValues()) {
            throw new VertexiumException("Previous property values were not included in fetch hints");
        }
        return previousValue;
    }

    public Object getValue() {
        if (!getFetchHints().isIncludePropertyValues()) {
            throw new VertexiumException("Property values were not included in fetch hints");
        }
        return value;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return String.format(
            "%s, value=%s, previousValue=%s, metadata=%s}",
            super.toString(),
            getValue(),
            getPreviousValue(),
            getMetadata()
        );
    }
}
