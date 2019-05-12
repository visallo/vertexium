package org.vertexium.accumulo.iterator;


import com.google.common.base.Joiner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowEncodingIterator;
import org.vertexium.accumulo.iterator.model.*;
import org.vertexium.accumulo.iterator.model.historicalEvents.*;
import org.vertexium.accumulo.iterator.util.ArrayUtils;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;
import org.vertexium.accumulo.iterator.util.OptionsUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HistoricalEventsIterator extends RowEncodingIterator {
    private static final String SETTING_FETCH_HINTS_PREFIX = "fetchHints.";
    private ElementType elementType;
    private IteratorHistoricalEventsFetchHints fetchHints;
    private HistoricalEventId after;

    @Override
    public SortedMap<Key, Value> rowDecoder(Key rowKey, Value rowValue) {
        throw new VertexiumAccumuloIteratorException("not implemented");
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        fetchHints = new IteratorHistoricalEventsFetchHints(
            OptionsUtils.parseLongOptional(options.get(SETTING_FETCH_HINTS_PREFIX + "startTime")),
            OptionsUtils.parseLongOptional(options.get(SETTING_FETCH_HINTS_PREFIX + "endTime")),
            OptionsUtils.parseSortDirectionOptional(options.get(SETTING_FETCH_HINTS_PREFIX + "sortDirection")),
            OptionsUtils.parseLongOptional(options.get(SETTING_FETCH_HINTS_PREFIX + "limit")),
            Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includePreviousPropertyValues")),
            Boolean.parseBoolean(options.get(SETTING_FETCH_HINTS_PREFIX + "includePropertyValues"))
        );
        elementType = OptionsUtils.parseElementTypeRequired(options.get("elementType"));
        String after = options.get("after");
        this.after = after == null || after.length() == 0 ? null : HistoricalEventId.fromString(after);
    }

    public static void setFetchHints(IteratorSetting settings, IteratorHistoricalEventsFetchHints fetchHints) {
        settings.addOption(SETTING_FETCH_HINTS_PREFIX + "startTime", fetchHints.getStartTime() == null ? "" : fetchHints.getStartTime().toString());
        settings.addOption(SETTING_FETCH_HINTS_PREFIX + "endTime", fetchHints.getEndTime() == null ? "" : fetchHints.getEndTime().toString());
        settings.addOption(SETTING_FETCH_HINTS_PREFIX + "sortDirection", fetchHints.getSortDirection().name());
        settings.addOption(SETTING_FETCH_HINTS_PREFIX + "limit", fetchHints.getLimit() == null ? "" : fetchHints.getLimit().toString());
        settings.addOption(SETTING_FETCH_HINTS_PREFIX + "includePreviousPropertyValues", fetchHints.isIncludePreviousPropertyValues() ? "true" : "false");
        settings.addOption(SETTING_FETCH_HINTS_PREFIX + "includePropertyValues", fetchHints.isIncludePropertyValues() ? "true" : "false");
    }

    public static void setElementType(IteratorSetting settings, ElementType elementType) {
        settings.addOption("elementType", elementType.name());
    }

    public static void setAfter(IteratorSetting settings, HistoricalEventId after) {
        settings.addOption("after", after == null ? "" : after.toString());
    }

    @Override
    public Value rowEncoder(List<Key> keys, List<Value> values) throws IOException {
        List<KeyValue> rows = toKeyValueList(keys, values);
        List<KeyValue> rowsList = rows.stream()
            .sorted(Comparator.comparingLong(KeyValue::getTimestamp).thenComparing(this::getKeyValueOrder))
            .collect(Collectors.toList());
        String elementId = keys.get(0).getRow().toString();

        HistoricalEventState state = new HistoricalEventState(elementType, elementId, fetchHints);

        for (KeyValue r : rowsList) {
            long timestamp = r.getTimestamp();

            if (elementType == ElementType.VERTEX && r.columnFamilyEquals(VertexIterator.CF_SIGNAL_BYTES)) {
                state.emitPropertyEvent();
                state.visibility = r.takeColumnVisibilityByteSequence();
                state.timestamp = timestamp;
                if (r.isSignalValueDeleted()) {
                    state.emitDeleteVertex();
                } else {
                    state.emitAddVertex(r);
                }
            } else if (elementType == ElementType.EDGE && r.columnFamilyEquals(EdgeIterator.CF_IN_VERTEX_BYTES)) {
                state.emitPropertyEvent();
                state.inVertexId = r.takeColumnQualifierByteSequence();
                state.edgeInVertexIds.put(elementId, state.inVertexId);
                state.emitAddOrAlterEdge(r);
            } else if (elementType == ElementType.EDGE && r.columnFamilyEquals(EdgeIterator.CF_OUT_VERTEX_BYTES)) {
                state.emitPropertyEvent();
                state.outVertexId = r.takeColumnQualifierByteSequence();
                state.edgeOutVertexIds.put(elementId, state.outVertexId);
                state.emitAddOrAlterEdge(r);
            } else if (elementType == ElementType.EDGE && r.columnFamilyEquals(EdgeIterator.CF_SIGNAL_BYTES)) {
                state.emitPropertyEvent();
                state.edgeLabel = r.takeColumnQualifierByteSequence();
                state.edgeEdgeLabels.put(elementId, state.edgeLabel);
                state.timestamp = timestamp;
                state.visibility = r.takeColumnVisibilityByteSequence();
                if (r.isSignalValueDeleted()) {
                    state.emitDeleteEdge();
                } else {
                    state.emitAddOrAlterEdge(r);
                }
            } else if (r.columnFamilyEquals(ElementIterator.CF_PROPERTY_BYTES)) {
                state.emitPropertyEvent();
                PropertyColumnQualifierByteSequence propertyColumnQualifier = new PropertyColumnQualifierByteSequence(r.takeColumnQualifierByteSequence());
                state.propertyKey = propertyColumnQualifier.getPropertyKey();
                state.propertyName = propertyColumnQualifier.getPropertyName();
                state.propertyVisibility = r.takeColumnVisibilityByteSequence();
                if (fetchHints.isIncludePropertyValues()) {
                    state.propertyValue = r.takeValue();
                } else {
                    state.propertyValue = null;
                }
                state.propertyMetadata = new IteratorMapMetadata();
                state.propertyTimestamp = timestamp;
            } else if (r.columnFamilyEquals(ElementIterator.CF_PROPERTY_METADATA_BYTES)) {
                PropertyMetadataColumnQualifierByteSequence propertyMetadataColumnQualifier = new PropertyMetadataColumnQualifierByteSequence(r.takeColumnQualifierByteSequence());
                if (state.propertyTimestamp != null
                    && propertyMetadataColumnQualifier.getPropertyKey().equals(state.propertyKey)
                    && propertyMetadataColumnQualifier.getPropertyName().equals(state.propertyName)
                    && propertyMetadataColumnQualifier.getPropertyVisibilityString().equals(state.propertyVisibility)) {
                    ByteSequence metadataKey = propertyMetadataColumnQualifier.getMetadataKey();
                    state.propertyMetadata.add(metadataKey, r.takeColumnVisibilityByteSequence(), r.takeValue());
                } else {
                    state.emitPropertyEvent();
                }
            } else if (r.columnFamilyEquals(ElementIterator.CF_PROPERTY_HIDDEN_BYTES)) {
                state.emitPropertyEvent();
                PropertyHiddenColumnQualifierByteSequence propertyHiddenColumnQualifier = new PropertyHiddenColumnQualifierByteSequence(r.takeColumnQualifierByteSequence());
                if (r.isHidden()) {
                    state.events.add(new IteratorHistoricalMarkPropertyHiddenEvent(
                        elementType,
                        state.elementId,
                        propertyHiddenColumnQualifier.getPropertyKey(),
                        propertyHiddenColumnQualifier.getPropertyName(),
                        propertyHiddenColumnQualifier.getPropertyVisibilityString(),
                        r.takeColumnVisibilityByteSequence(),
                        timestamp,
                        readHiddenValue(r)
                    ));
                } else {
                    state.events.add(new IteratorHistoricalMarkPropertyVisibleEvent(
                        elementType,
                        state.elementId,
                        propertyHiddenColumnQualifier.getPropertyKey(),
                        propertyHiddenColumnQualifier.getPropertyName(),
                        propertyHiddenColumnQualifier.getPropertyVisibilityString(),
                        r.takeColumnVisibilityByteSequence(),
                        timestamp,
                        readHiddenValue(r)
                    ));
                }
            } else if (r.columnFamilyEquals(ElementIterator.CF_HIDDEN_BYTES)) {
                state.emitPropertyEvent();
                state.clearElementSignal();
                if (r.isHidden()) {
                    state.events.add(new IteratorHistoricalMarkHiddenEvent(
                        elementType,
                        state.elementId,
                        r.takeColumnVisibilityByteSequence(),
                        timestamp,
                        readHiddenValue(r)
                    ));
                } else {
                    state.events.add(new IteratorHistoricalMarkVisibleEvent(
                        elementType,
                        state.elementId,
                        r.takeColumnVisibilityByteSequence(),
                        timestamp,
                        readHiddenValue(r)
                    ));
                }
            } else if (r.columnFamilyEquals(ElementIterator.CF_SOFT_DELETE_BYTES)) {
                state.emitPropertyEvent();
                state.clearElementSignal();
                if (elementType == ElementType.VERTEX) {
                    state.events.add(new IteratorHistoricalSoftDeleteVertexEvent(state.elementId, timestamp, readSoftDeleteValue(r)));
                } else if (elementType == ElementType.EDGE) {
                    ByteSequence outVertexId = state.edgeOutVertexIds.get(state.elementId);
                    ByteSequence inVertexId = state.edgeInVertexIds.get(state.elementId);
                    ByteSequence edgeLabel = state.edgeEdgeLabels.get(state.elementId);
                    state.events.add(new IteratorHistoricalSoftDeleteEdgeEvent(state.elementId, outVertexId, inVertexId, edgeLabel, timestamp, readSoftDeleteValue(r)));
                } else {
                    throw new VertexiumAccumuloIteratorException("Unhandled element type: " + elementType);
                }
            } else if (r.columnFamilyEquals(ElementIterator.CF_PROPERTY_SOFT_DELETE_BYTES)) {
                state.emitPropertyEvent();
                PropertyColumnQualifierByteSequence propertyColumnQualifier = new PropertyColumnQualifierByteSequence(r.takeColumnQualifierByteSequence());
                state.events.add(new IteratorHistoricalSoftDeletePropertyEvent(
                    elementType,
                    state.elementId,
                    propertyColumnQualifier.getPropertyKey(),
                    propertyColumnQualifier.getPropertyName(),
                    r.takeColumnVisibilityByteSequence(),
                    timestamp,
                    readSoftDeleteValue(r)
                ));
            } else if (elementType == ElementType.VERTEX && (r.columnFamilyEquals(VertexIterator.CF_OUT_EDGE_BYTES) || r.columnFamilyEquals(VertexIterator.CF_IN_EDGE_BYTES))) {
                state.emitPropertyEvent();
                EdgeInfo edgeInfo = EdgeInfo.parse(r.takeValue(), r.takeColumnVisibility(), timestamp);
                ByteSequence edgeId = r.takeColumnQualifierByteSequence();
                state.addEdgeInfo(edgeId, edgeInfo);
                state.events.add(new IteratorHistoricalAddEdgeToVertexEvent(
                    state.elementId,
                    edgeId,
                    r.columnFamilyEquals(VertexIterator.CF_OUT_EDGE_BYTES) ? Direction.OUT : Direction.IN,
                    edgeInfo.getLabel(),
                    edgeInfo.getVertexId(),
                    r.takeColumnVisibilityByteSequence(),
                    timestamp
                ));
            } else if (elementType == ElementType.VERTEX && (r.columnFamilyEquals(VertexIterator.CF_OUT_EDGE_SOFT_DELETE_BYTES) || r.columnFamilyEquals(VertexIterator.CF_IN_EDGE_SOFT_DELETE_BYTES))) {
                state.emitPropertyEvent();
                ByteSequence edgeId = r.takeColumnQualifierByteSequence();
                EdgeInfo edgeInfo = state.edgeInfos.get(edgeId);
                state.events.add(new IteratorHistoricalSoftDeleteEdgeToVertexEvent(
                    state.elementId,
                    edgeId,
                    r.columnFamilyEquals(VertexIterator.CF_OUT_EDGE_SOFT_DELETE_BYTES) ? Direction.OUT : Direction.IN,
                    edgeInfo == null ? null : edgeInfo.getLabel(),
                    edgeInfo == null ? null : edgeInfo.getVertexId(),
                    r.takeColumnVisibilityByteSequence(),
                    timestamp,
                    readSoftDeleteValue(r)
                ));
            } else {
                r.toString();
            }
        }
        state.emitPropertyEvent();

        return IteratorHistoricalEvent.encode(applyToResults(state.events.stream(), fetchHints, after).collect(Collectors.toList()));
    }

    private Value readSoftDeleteValue(KeyValue r) {
        Value v = r.takeValue();
        if (ElementIterator.SOFT_DELETE_VALUE.equals(v)) {
            return null;
        }
        return v;
    }

    private static Value readSignalValue(KeyValue r) {
        Value v = r.peekValue();
        byte[] signalValueDeletedArray = ElementIterator.SIGNAL_VALUE_DELETED.get();
        if (ArrayUtils.startsWith(v.get(), signalValueDeletedArray)) {
            byte[] data = new byte[v.get().length - signalValueDeletedArray.length];
            System.arraycopy(v.get(), signalValueDeletedArray.length, data, 0, data.length);
            return new Value(data);
        } else {
            return r.takeValue();
        }
    }

    private Value readHiddenValue(KeyValue r) {
        Value v = r.peekValue();
        if (ElementIterator.HIDDEN_VALUE.equals(v) || ElementIterator.HIDDEN_VALUE_DELETED.equals(v)) {
            return null;
        }
        byte[] hiddenValueDeletedArray = ElementIterator.HIDDEN_VALUE_DELETED.get();
        if (ArrayUtils.startsWith(v.get(), hiddenValueDeletedArray)) {
            byte[] data = new byte[v.get().length - hiddenValueDeletedArray.length];
            System.arraycopy(v.get(), hiddenValueDeletedArray.length, data, 0, data.length);
            return new Value(data);
        } else {
            return r.takeValue();
        }
    }

    private String getKeyValueOrder(KeyValue r) {
        if (r.columnFamilyEquals(EdgeIterator.CF_IN_VERTEX_BYTES)
            || r.columnFamilyEquals(EdgeIterator.CF_OUT_VERTEX_BYTES)) {
            return "a";
        }
        if (r.columnFamilyEquals(VertexIterator.CF_SIGNAL_BYTES)) {
            return "b";
        }
        if (r.columnFamilyEquals(ElementIterator.CF_PROPERTY_BYTES)) {
            PropertyColumnQualifierByteSequence propertyColumnQualifier = new PropertyColumnQualifierByteSequence(r.peekColumnQualifierByteSequence());
            return String.format(
                "c_%s_%s_%s",
                ByteSequenceUtils.toHexString(propertyColumnQualifier.getPropertyName()),
                ByteSequenceUtils.toHexString(propertyColumnQualifier.getPropertyKey()),
                ByteSequenceUtils.toHexString(r.peekColumnVisibilityByteSequence())
            );
        }
        if (r.columnFamilyEquals(ElementIterator.CF_PROPERTY_METADATA_BYTES)) {
            PropertyMetadataColumnQualifierByteSequence propertyMetadataColumnQualifier = new PropertyMetadataColumnQualifierByteSequence(r.peekColumnQualifierByteSequence());
            return String.format(
                "c_%s_%s_%s_z",
                ByteSequenceUtils.toHexString(propertyMetadataColumnQualifier.getPropertyName()),
                ByteSequenceUtils.toHexString(propertyMetadataColumnQualifier.getPropertyKey()),
                ByteSequenceUtils.toHexString(propertyMetadataColumnQualifier.getPropertyVisibilityString())
            );
        }
        return "z";
    }

    private Stream<IteratorHistoricalEvent> applyToResults(
        Stream<IteratorHistoricalEvent> events,
        IteratorHistoricalEventsFetchHints fetchHints,
        HistoricalEventId after
    ) {
        switch (fetchHints.getSortDirection()) {
            case ASCENDING:
                events = events.sorted();
                break;
            case DESCENDING:
                events = events.sorted((o1, o2) -> -o1.compareTo(o2));
                break;
            default:
                throw new VertexiumAccumuloIteratorException("Unhandled sort direction: " + fetchHints.getSortDirection());
        }

        if (fetchHints.getStartTime() != null || fetchHints.getEndTime() != null) {
            long startTimeMillis = fetchHints.getStartTime() == null ? 0 : fetchHints.getStartTime();
            long endTimeMillis = fetchHints.getEndTime() == null ? Long.MAX_VALUE : fetchHints.getEndTime();
            events = events.filter(event -> {
                long ts = event.getTimestamp();
                if (ts < startTimeMillis) {
                    return false;
                }
                if (ts > endTimeMillis) {
                    return false;
                }
                return true;
            });
        }

        if (after != null) {
            events = events.filter(event -> {
                int i = event.getHistoricalEventId().compareTo(after);
                switch (fetchHints.getSortDirection()) {
                    case ASCENDING:
                        return i > 0;
                    case DESCENDING:
                        return i < 0;
                    default:
                        throw new VertexiumAccumuloIteratorException("Unhandled sort direction: " + fetchHints.getSortDirection());
                }
            });
        }

        if (fetchHints.getLimit() != null) {
            events = events.limit(fetchHints.getLimit());
        }

        return events;
    }

    public static List<IteratorHistoricalEvent> decode(Value value, String elementId) throws IOException {
        return IteratorHistoricalEvent.decode(value, elementId);
    }

    private List<KeyValue> toKeyValueList(List<Key> keys, List<Value> values) {
        List<KeyValue> rows = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            Value value = values.get(i);
            KeyValue keyValue = new KeyValue();
            keyValue.set(key, value);
            rows.add(keyValue);
        }
        return rows;
    }

    private static class HistoricalEventState {
        private final ElementType elementType;
        private final String elementId;
        private final IteratorHistoricalEventsFetchHints fetchHints;
        final List<IteratorHistoricalEvent> events = new ArrayList<>();
        ByteSequence visibility;
        Long timestamp;
        ByteSequence inVertexId;
        ByteSequence outVertexId;
        ByteSequence edgeLabel;
        ByteSequence propertyKey;
        ByteSequence propertyName;
        ByteSequence propertyVisibility;
        Value propertyValue;
        IteratorMapMetadata propertyMetadata;
        Long propertyTimestamp;
        ByteSequence lastVisibility;
        ByteSequence lastOutVertexId;
        ByteSequence lastInVertexId;
        ByteSequence lastEdgeLabel;
        final Map<String, Value> previousPropertyValues = new HashMap<>();
        final Map<String, Long> previousPropertyValueTimestamps = new HashMap<>();
        final Map<ByteSequence, EdgeInfo> edgeInfos = new HashMap<>();
        final Map<String, ByteSequence> edgeOutVertexIds = new HashMap<>();
        final Map<String, ByteSequence> edgeInVertexIds = new HashMap<>();
        final Map<String, ByteSequence> edgeEdgeLabels = new HashMap<>();

        HistoricalEventState(ElementType elementType, String elementId, IteratorHistoricalEventsFetchHints fetchHints) {
            this.elementType = elementType;
            this.elementId = elementId;
            this.fetchHints = fetchHints;
        }

        void emitPropertyEvent() {
            if (propertyTimestamp != null) {
                String previousValueKey = Joiner.on("_").join(
                    elementType,
                    propertyKey,
                    propertyName
                );
                Long previousValueTimestamp = fetchHints.isIncludePreviousPropertyValues()
                    ? previousPropertyValueTimestamps.get(previousValueKey)
                    : null;
                Value previousValue = fetchHints.isIncludePreviousPropertyValues()
                    ? previousPropertyValues.get(previousValueKey)
                    : null;
                events.add(new IteratorHistoricalAddPropertyEvent(
                    elementType,
                    elementId,
                    propertyKey,
                    propertyName,
                    propertyVisibility,
                    previousValueTimestamp,
                    previousValue,
                    propertyValue,
                    propertyMetadata,
                    propertyTimestamp
                ));
                previousPropertyValueTimestamps.put(previousValueKey, propertyTimestamp);
                previousPropertyValues.put(previousValueKey, propertyValue);
                propertyKey = null;
                propertyName = null;
                propertyVisibility = null;
                propertyMetadata = null;
                propertyTimestamp = null;
            }
        }

        void emitDeleteEdge() {
            events.add(new IteratorHistoricalDeleteEdgeEvent(
                elementId,
                outVertexId,
                inVertexId,
                edgeLabel,
                visibility,
                timestamp
            ));
            lastOutVertexId = outVertexId;
            lastInVertexId = inVertexId;
            lastEdgeLabel = edgeLabel;
            lastVisibility = visibility;
        }

        void emitAddOrAlterEdge(KeyValue r) {
            if (outVertexId != null && inVertexId != null && edgeLabel != null) {
                if (shouldEmitAddOrAlterEdge()) {
                    if (events.size() > 0) {
                        IteratorHistoricalEvent lastEvent = events.get(events.size() - 1);
                        if (lastEvent instanceof IteratorHistoricalDeleteEdgeEvent) {
                            IteratorHistoricalDeleteEdgeEvent deleteEdgeEvent = (IteratorHistoricalDeleteEdgeEvent) lastEvent;
                            if (timestamp - deleteEdgeEvent.getTimestamp() < 100) {
                                events.remove(events.size() - 1);
                                Value data = readSignalValue(r);
                                events.add(new IteratorHistoricalAlterEdgeVisibilityEvent(
                                    elementId,
                                    outVertexId,
                                    inVertexId,
                                    edgeLabel,
                                    deleteEdgeEvent.getVisibility(),
                                    visibility,
                                    timestamp,
                                    data
                                ));
                                lastVisibility = visibility;
                                return;
                            }
                        }
                    }

                    if (lastEdgeLabel == null || lastEdgeLabel.equals(edgeLabel)) {
                        events.add(new IteratorHistoricalAddEdgeEvent(
                            elementId,
                            outVertexId,
                            inVertexId,
                            edgeLabel,
                            visibility,
                            timestamp
                        ));
                    } else {
                        events.add(new IteratorHistoricalAlterEdgeLabelEvent(elementId, edgeLabel, timestamp));
                    }
                    lastOutVertexId = outVertexId;
                    lastInVertexId = inVertexId;
                    lastEdgeLabel = edgeLabel;
                    lastVisibility = visibility;
                }
            }
        }

        private boolean shouldEmitAddOrAlterEdge() {
            if (lastOutVertexId == null
                || lastInVertexId == null
                || lastEdgeLabel == null
                || lastVisibility == null
            ) {
                return true;
            }
            if (lastOutVertexId.equals(outVertexId)
                && lastInVertexId.equals(inVertexId)
                && lastEdgeLabel.equals(edgeLabel)
                && lastVisibility.equals(visibility)
            ) {
                return false;
            }
            return true;
        }

        public void emitDeleteVertex() {
            events.add(new IteratorHistoricalDeleteVertexEvent(elementId, visibility, timestamp));
            lastVisibility = visibility;
        }

        void emitAddVertex(KeyValue r) {
            if (visibility == null) {
                throw new VertexiumAccumuloIteratorException("visibility cannot be null");
            }
            if (timestamp == null) {
                throw new VertexiumAccumuloIteratorException("timestamp cannot be null");
            }
            if (shouldEmitAddVertex()) {
                if (events.size() > 0) {
                    IteratorHistoricalEvent lastEvent = events.get(events.size() - 1);
                    if (lastEvent instanceof IteratorHistoricalDeleteVertexEvent) {
                        IteratorHistoricalDeleteVertexEvent deleteVertexEvent = (IteratorHistoricalDeleteVertexEvent) lastEvent;
                        if (timestamp - deleteVertexEvent.getTimestamp() < 100) {
                            Value data = readSignalValue(r);
                            events.remove(events.size() - 1);
                            events.add(new IteratorHistoricalAlterVertexVisibilityEvent(
                                elementId,
                                deleteVertexEvent.getVisibility(),
                                visibility,
                                timestamp,
                                data
                            ));
                            lastVisibility = visibility;
                            return;
                        }
                    }
                }
                events.add(new IteratorHistoricalAddVertexEvent(elementId, visibility, timestamp));
                lastVisibility = visibility;
            }
        }

        private boolean shouldEmitAddVertex() {
            if (lastVisibility == null) {
                return true;
            }
            if (lastVisibility.equals(visibility)) {
                return false;
            }
            return true;
        }

        void clearElementSignal() {
            visibility = null;
            timestamp = null;
            inVertexId = null;
            outVertexId = null;
            edgeLabel = null;
            lastEdgeLabel = null;
            lastInVertexId = null;
            lastOutVertexId = null;
            lastVisibility = null;
        }

        public void addEdgeInfo(ByteSequence edgeId, EdgeInfo edgeInfo) {
            edgeInfos.put(edgeId, edgeInfo);
        }
    }
}
