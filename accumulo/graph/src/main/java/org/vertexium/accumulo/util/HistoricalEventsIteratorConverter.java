package org.vertexium.accumulo.util;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.*;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.iterator.model.SortDirection;
import org.vertexium.accumulo.iterator.model.historicalEvents.HistoricalEventId;
import org.vertexium.accumulo.iterator.model.historicalEvents.*;
import org.vertexium.historicalEvent.*;
import org.vertexium.property.StreamingPropertyValueRef;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

public class HistoricalEventsIteratorConverter {
    public static HistoricalEvent convert(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorHistoricalEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        if (iterEvent == null) {
            throw new VertexiumException("Unhandled iterator event type: null");
        }
        if (iterEvent instanceof IteratorHistoricalAddEdgeEvent) {
            return convertHistoricalAddEdgeEvent(graph, elementId, (IteratorHistoricalAddEdgeEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalAddEdgeToVertexEvent) {
            return convertHistoricalAddEdgeToVertexEvent(graph, elementId, (IteratorHistoricalAddEdgeToVertexEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalAddPropertyEvent) {
            return convertHistoricalAddPropertyEvent(
                graph,
                elementType,
                elementId,
                (IteratorHistoricalAddPropertyEvent) iterEvent,
                fetchHints
            );
        } else if (iterEvent instanceof IteratorHistoricalAddVertexEvent) {
            return convertHistoricalAddVertexEvent(elementId, (IteratorHistoricalAddVertexEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalAlterEdgeLabelEvent) {
            return convertHistoricalAlterEdgeLabelEvent(graph, elementId, (IteratorHistoricalAlterEdgeLabelEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalMarkHiddenEvent) {
            return convertHistoricalMarkHiddenEvent(graph, elementType, elementId, (IteratorHistoricalMarkHiddenEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalMarkPropertyHiddenEvent) {
            return convertHistoricalMarkPropertyHiddenEvent(graph, elementType, elementId, (IteratorHistoricalMarkPropertyHiddenEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalMarkPropertyVisibleEvent) {
            return convertHistoricalMarkPropertyVisibleEvent(graph, elementType, elementId, (IteratorHistoricalMarkPropertyVisibleEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalMarkVisibleEvent) {
            return convertHistoricalMarkVisibleEvent(graph, elementType, elementId, (IteratorHistoricalMarkVisibleEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalSoftDeleteEdgeToVertexEvent) {
            return convertHistoricalSoftDeleteEdgeToVertexEvent(graph, elementId, (IteratorHistoricalSoftDeleteEdgeToVertexEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalSoftDeleteVertexEvent) {
            return convertHistoricalSoftDeleteVertexEvent(graph, elementId, (IteratorHistoricalSoftDeleteVertexEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalSoftDeleteEdgeEvent) {
            return convertHistoricalSoftDeleteEdgeEvent(graph, elementId, (IteratorHistoricalSoftDeleteEdgeEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalSoftDeletePropertyEvent) {
            return convertHistoricalSoftDeletePropertyEvent(graph, elementType, elementId, (IteratorHistoricalSoftDeletePropertyEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalDeleteVertexEvent) {
            return convertHistoricalDeleteVertexEvent(elementId, (IteratorHistoricalDeleteVertexEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalDeleteEdgeEvent) {
            return convertHistoricalDeleteEdgeEvent(graph, elementId, (IteratorHistoricalDeleteEdgeEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalAlterVertexVisibilityEvent) {
            return convertHistoricalAlterVertexVisibilityEvent(graph, elementId, (IteratorHistoricalAlterVertexVisibilityEvent) iterEvent, fetchHints);
        } else if (iterEvent instanceof IteratorHistoricalAlterEdgeVisibilityEvent) {
            return convertHistoricalAlterEdgeVisibilityEvent(graph, elementId, (IteratorHistoricalAlterEdgeVisibilityEvent) iterEvent, fetchHints);
        } else {
            throw new VertexiumException("Unhandled iterator event type: " + iterEvent.getClass().getName());
        }
    }

    private static HistoricalSoftDeletePropertyEvent convertHistoricalSoftDeletePropertyEvent(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorHistoricalSoftDeletePropertyEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalSoftDeletePropertyEvent(
            elementType,
            elementId,
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyKey()),
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyName()),
            byteSequenceToVisibility(iterEvent.getPropertyVisibilityString()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, elementType, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalSoftDeleteElementEvent convertHistoricalSoftDeleteVertexEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalSoftDeleteVertexEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalSoftDeleteVertexEvent(
            elementId,
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, ElementType.VERTEX, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalSoftDeleteEdgeEvent convertHistoricalSoftDeleteEdgeEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalSoftDeleteEdgeEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalSoftDeleteEdgeEvent(
            elementId,
            byteSequenceToString(iterEvent.getOutVertexId()),
            byteSequenceToString(iterEvent.getInVertexId()),
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getEdgeLabel()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, ElementType.EDGE, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalSoftDeleteEdgeToVertexEvent convertHistoricalSoftDeleteEdgeToVertexEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalSoftDeleteEdgeToVertexEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalSoftDeleteEdgeToVertexEvent(
            elementId,
            byteSequenceToString(iterEvent.getEdgeId()),
            iteratorDirectionToDirection(iterEvent.getEdgeDirection()),
            graph.getNameSubstitutionStrategy().inflate(new String(iterEvent.getEdgeLabelBytes(), StandardCharsets.UTF_8)),
            new String(iterEvent.getOtherVertexIdBytes(), StandardCharsets.UTF_8),
            byteSequenceToVisibility(iterEvent.getEdgeVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, ElementType.VERTEX, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalMarkVisibleEvent convertHistoricalMarkVisibleEvent(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorHistoricalMarkVisibleEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkVisibleEvent(
            elementType,
            elementId,
            byteSequenceToVisibility(iterEvent.getHiddenVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, elementType, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalMarkPropertyVisibleEvent convertHistoricalMarkPropertyVisibleEvent(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorHistoricalMarkPropertyVisibleEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkPropertyVisibleEvent(
            elementType,
            elementId,
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyKey()),
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyName()),
            byteSequenceToVisibility(iterEvent.getPropertyVisibilityString()),
            byteSequenceToVisibility(iterEvent.getHiddenVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, elementType, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalMarkPropertyHiddenEvent convertHistoricalMarkPropertyHiddenEvent(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorHistoricalMarkPropertyHiddenEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkPropertyHiddenEvent(
            elementType,
            elementId,
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyKey()),
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyName()),
            byteSequenceToVisibility(iterEvent.getPropertyVisibilityString()),
            byteSequenceToVisibility(iterEvent.getHiddenVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, elementType, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalMarkHiddenEvent convertHistoricalMarkHiddenEvent(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorHistoricalMarkHiddenEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkHiddenEvent(
            elementType,
            elementId,
            byteSequenceToVisibility(iterEvent.getHiddenVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, elementType, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalAlterEdgeLabelEvent convertHistoricalAlterEdgeLabelEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalAlterEdgeLabelEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAlterEdgeLabelEvent(
            elementId,
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getEdgeLabel()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalEvent convertHistoricalDeleteVertexEvent(
        String elementId,
        IteratorHistoricalDeleteVertexEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalDeleteVertexEvent(
            elementId,
            byteSequenceToVisibility(iterEvent.getVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalEvent convertHistoricalAlterVertexVisibilityEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalAlterVertexVisibilityEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAlterVertexVisibilityEvent(
            elementId,
            byteSequenceToVisibility(iterEvent.getOldVisibility()),
            byteSequenceToVisibility(iterEvent.getNewVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, ElementType.VERTEX, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalAddVertexEvent convertHistoricalAddVertexEvent(
        String elementId,
        IteratorHistoricalAddVertexEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAddVertexEvent(
            elementId,
            byteSequenceToVisibility(iterEvent.getVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalAddPropertyEvent convertHistoricalAddPropertyEvent(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorHistoricalAddPropertyEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAddPropertyEvent(
            elementType,
            elementId,
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyKey()),
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getPropertyName()),
            byteSequenceToVisibility(iterEvent.getPropertyVisibilityString()),
            valueToObject(graph, elementType, elementId, iterEvent.getPreviousValue(), iterEvent.getPreviousValueTimestamp()),
            valueToObject(graph, elementType, elementId, iterEvent.getValue(), iterEvent.getTimestamp()),
            iteratorMapMetadataToMetadata(graph, elementType, elementId, iterEvent.getPropertyMetadata(), iterEvent.getTimestamp()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalEvent convertHistoricalAlterEdgeVisibilityEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalAlterEdgeVisibilityEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAlterEdgeVisibilityEvent(
            elementId,
            byteSequenceToString(iterEvent.getOutVertexId()),
            byteSequenceToString(iterEvent.getInVertexId()),
            byteSequenceToString(iterEvent.getEdgeLabel()),
            byteSequenceToVisibility(iterEvent.getOldVisibility()),
            byteSequenceToVisibility(iterEvent.getNewVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            valueToObject(graph, ElementType.EDGE, elementId, iterEvent.getData(), iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalAddEdgeToVertexEvent convertHistoricalAddEdgeToVertexEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalAddEdgeToVertexEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAddEdgeToVertexEvent(
            elementId,
            byteSequenceToString(iterEvent.getEdgeId()),
            iteratorDirectionToDirection(iterEvent.getEdgeDirection()),
            graph.getNameSubstitutionStrategy().inflate(new String(iterEvent.getEdgeLabelBytes(), StandardCharsets.UTF_8)),
            new String(iterEvent.getOtherVertexIdBytes(), StandardCharsets.UTF_8),
            byteSequenceToVisibility(iterEvent.getEdgeVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalEvent convertHistoricalDeleteEdgeEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalDeleteEdgeEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalDeleteEdgeEvent(
            elementId,
            byteSequenceToString(iterEvent.getOutVertexId()),
            byteSequenceToString(iterEvent.getInVertexId()),
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getEdgeLabel()),
            byteSequenceToVisibility(iterEvent.getVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static HistoricalAddEdgeEvent convertHistoricalAddEdgeEvent(
        AccumuloGraph graph,
        String elementId,
        IteratorHistoricalAddEdgeEvent iterEvent,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAddEdgeEvent(
            elementId,
            byteSequenceToString(iterEvent.getOutVertexId()),
            byteSequenceToString(iterEvent.getInVertexId()),
            graph.getNameSubstitutionStrategy().inflate(iterEvent.getEdgeLabel()),
            byteSequenceToVisibility(iterEvent.getVisibility()),
            longToZonedDateTime(iterEvent.getTimestamp()),
            fetchHints
        );
    }

    private static Direction iteratorDirectionToDirection(org.vertexium.accumulo.iterator.model.Direction direction) {
        switch (direction) {
            case IN:
                return Direction.IN;
            case OUT:
                return Direction.OUT;
            default:
                throw new VertexiumException("Unhandled iterator direction: " + direction);
        }
    }

    private static Metadata iteratorMapMetadataToMetadata(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        IteratorMapMetadata iterMetadata,
        long timestamp
    ) {
        MapMetadata results = new MapMetadata();
        for (Map.Entry<ByteSequence, Map<ByteSequence, Value>> byKey : iterMetadata.getValues().entrySet()) {
            String key = graph.getNameSubstitutionStrategy().inflate(byKey.getKey());
            for (Map.Entry<ByteSequence, Value> byVisibility : byKey.getValue().entrySet()) {
                Visibility visibility = byteSequenceToVisibility(byVisibility.getKey());
                Object value = valueToObject(graph, elementType, elementId, byVisibility.getValue(), timestamp);
                results.add(key, value, visibility);
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private static Object valueToObject(
        AccumuloGraph graph,
        ElementType elementType,
        String elementId,
        Value value,
        Long timestamp
    ) {
        if (value == null) {
            return null;
        }
        Object propertyValue = graph.getVertexiumSerializer().bytesToObject(elementType, elementId, value.get());
        if (propertyValue instanceof StreamingPropertyValueRef) {
            propertyValue = ((StreamingPropertyValueRef) propertyValue).toStreamingPropertyValue(graph, timestamp);
        }
        return propertyValue;
    }

    private static ZonedDateTime longToZonedDateTime(long timestamp) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
    }

    private static Visibility byteSequenceToVisibility(ByteSequence byteSequence) {
        return new Visibility(byteSequenceToString(byteSequence));
    }

    private static String byteSequenceToString(ByteSequence byteSequence) {
        return new String(byteSequence.toArray());
    }

    public static org.vertexium.accumulo.iterator.model.ElementType convertToIteratorElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return org.vertexium.accumulo.iterator.model.ElementType.VERTEX;
            case EDGE:
                return org.vertexium.accumulo.iterator.model.ElementType.EDGE;
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    public static IteratorHistoricalEventsFetchHints convertToIteratorHistoricalEventsFetchHints(HistoricalEventsFetchHints fetchHints) {
        return new IteratorHistoricalEventsFetchHints(
            zonedDateTimeToLong(fetchHints.getStartTime()),
            zonedDateTimeToLong(fetchHints.getEndTime()),
            convertToIteratorSortDirection(fetchHints.getSortDirection()),
            fetchHints.getLimit(),
            fetchHints.isIncludePreviousPropertyValues(),
            fetchHints.isIncludePropertyValues()
        );
    }

    private static SortDirection convertToIteratorSortDirection(HistoricalEventsFetchHints.SortDirection sortDirection) {
        switch (sortDirection) {
            case ASCENDING:
                return SortDirection.ASCENDING;
            case DESCENDING:
                return SortDirection.DESCENDING;
            default:
                throw new VertexiumException("Unhandled sort direction: " + sortDirection);
        }
    }

    private static Long zonedDateTimeToLong(ZonedDateTime t) {
        if (t == null) {
            return null;
        }
        return t.toInstant().toEpochMilli();
    }

    public static HistoricalEventId convertToIteratorHistoricalEventId(org.vertexium.historicalEvent.HistoricalEventId after) {
        if (after == null) {
            return null;
        }
        return new HistoricalEventId(
            after.getTimestamp().toInstant().toEpochMilli(),
            convertToIteratorElementType(after.getElementType()),
            after.getElementId(),
            after.getSubOrder()
        );
    }
}
