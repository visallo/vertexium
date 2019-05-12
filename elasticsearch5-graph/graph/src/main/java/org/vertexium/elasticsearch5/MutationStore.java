package org.vertexium.elasticsearch5;

import com.google.common.base.Joiner;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.*;
import org.vertexium.elasticsearch5.models.*;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;
import org.vertexium.elasticsearch5.utils.SearchResponseUtils;
import org.vertexium.historicalEvent.*;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class MutationStore {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(MutationStore.class);
    private final Elasticsearch5Graph graph;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final ScriptService scriptService;

    public MutationStore(
        Elasticsearch5Graph graph,
        IndexSelectionStrategy indexSelectionStrategy,
        ScriptService scriptService
    ) {
        this.graph = graph;
        this.indexSelectionStrategy = indexSelectionStrategy;
        this.scriptService = scriptService;
    }

    public Edge getEdge(String edgeId, FetchHints fetchHints, long endTime, User user) {
        return Elasticsearch5GraphEdge.createFromMutations(
            graph,
            edgeId,
            getMutations(ElementType.EDGE, edgeId, endTime),
            fetchHints,
            user,
            scriptService
        );
    }

    public Vertex getVertex(String vertexId, FetchHints fetchHints, long endTime, User user) {
        return Elasticsearch5GraphVertex.createFromMutations(
            graph,
            vertexId,
            getMutations(ElementType.VERTEX, vertexId, endTime),
            fetchHints,
            user,
            scriptService
        );
    }

    public Stream<Edge> getEdges(
        String outVertex,
        String inVertex,
        Direction direction,
        String[] labels,
        FetchHints fetchHints,
        long endTime,
        User user
    ) {
        Map<String, List<SearchHit>> mutationsByElementId = stream(getEdgeMutations(outVertex, inVertex, direction, labels, endTime))
            .collect(Collectors.groupingBy(m -> (String) m.getSource().get(FieldNames.MUTATION_ELEMENT_ID)));
        return mutationsByElementId.entrySet().stream()
            .map(entry -> {
                String elementId = entry.getKey();
                return Elasticsearch5GraphEdge.createFromMutations(
                    graph,
                    elementId,
                    entry.getValue(),
                    fetchHints,
                    user,
                    scriptService
                );
            });
    }

    private Iterable<SearchHit> getMutations(ElementType elementType, String elementId, long endTime) {
        LOGGER.debug("getMutations(elementType=%s, elementId=%s, endTime=%d)", elementType, elementId, endTime);
        String elementTypeString = ElasticsearchDocumentType.fromElementType(elementType).getKey();
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_TYPE, elementTypeString))
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_ID, elementId))
            .must(QueryBuilders.rangeQuery(FieldNames.MUTATION_TIMESTAMP).lte(endTime));
        SearchResponse response = graph.getClient()
            .prepareSearch(indexSelectionStrategy.getMutationIndexNames(graph))
            .setScroll(new TimeValue(60000))
            .addSort(FieldNames.MUTATION_TIMESTAMP, SortOrder.ASC)
            .setQuery(query)
            .get();
        return SearchResponseUtils.scrollToIterable(graph.getClient(), response);
    }

    private Iterable<SearchHit> getEdgeMutations(
        String vertexId1,
        String vertexId2,
        Direction direction,
        String[] labels,
        long endTime
    ) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "getEdgeMutations(vertexId1=%s, vertexId2=%s, direction=%s, labels=%s)",
                vertexId1,
                vertexId2,
                direction,
                Joiner.on(", ").join(labels)
            );
        }
        String elementTypeString = ElasticsearchDocumentType.EDGE.getKey();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_TYPE, elementTypeString))
            .must(QueryBuilders.rangeQuery(FieldNames.MUTATION_TIMESTAMP).lte(endTime));
        if (labels != null) {
            query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_EDGE_LABEL, labels));
        }
        switch (direction) {
            case OUT:
                if (vertexId1 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId2));
                }
                break;

            case IN:
                if (vertexId1 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId2));
                }
                break;

            case BOTH:
                BoolQueryBuilder v1to2 = QueryBuilders.boolQuery();
                if (vertexId1 != null) {
                    v1to2 = v1to2.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    v1to2 = v1to2.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId2));
                }

                BoolQueryBuilder v2to1 = QueryBuilders.boolQuery();
                if (vertexId1 != null) {
                    v2to1 = v2to1.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    v2to1 = v2to1.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId2));
                }

                BoolQueryBuilder q = QueryBuilders.boolQuery()
                    .should(v1to2)
                    .should(v2to1);
                query = query.must(q.minimumShouldMatch(1));
                break;
        }
        SearchResponse response = graph.getClient()
            .prepareSearch(indexSelectionStrategy.getMutationIndexNames(graph))
            .setScroll(new TimeValue(60000))
            .addSort(FieldNames.MUTATION_TIMESTAMP, SortOrder.ASC)
            .setQuery(query)
            .get();
        return SearchResponseUtils.scrollToIterable(graph.getClient(), response);
    }

    public Stream<HistoricalEvent> getHistoricalEventsForElement(
        ElementLocation elementLocation,
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        User user
    ) {
        LOGGER.debug("getHistoricalEventsForElement(elementLocation=%s, after=%s, fetchHints=%s)", elementLocation, after, fetchHints);
        String elementTypeString = ElasticsearchDocumentType.fromElementType(elementLocation.getElementType()).getKey();
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termsQuery(FieldNames.MUTATION_ELEMENT_ID, elementLocation.getId()))
            .must(QueryBuilders.termsQuery(FieldNames.MUTATION_ELEMENT_TYPE, elementTypeString));

        String indexName = indexSelectionStrategy.getMutationIndexName(graph, elementLocation);
        SearchResponse response = graph.getClient()
            .prepareSearch(indexName)
            .setScroll(new TimeValue(60000))
            .setQuery(query)
            .storedFields(FieldNames.MUTATION_DATA)
            .addSort(
                FieldNames.MUTATION_TIMESTAMP,
                fetchHints.getSortDirection() == HistoricalEventsFetchHints.SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC
            )
            .get();

        HistoricalEventState state = new HistoricalEventState();
        state.elementVisibility = null;
        state.edgeLabel = elementLocation instanceof EdgeElementLocation
            ? ((EdgeElementLocation) elementLocation).getLabel()
            : null;
        Stream<HistoricalEvent> hits = SearchResponseUtils.scrollToStream(graph.getClient(), response)
            .map(hit -> hitToHistoricalEvent(elementLocation, hit, fetchHints, state))
            .filter(Objects::nonNull);
        return fetchHints.applyToResults(hits, after);
    }

    private HistoricalEvent hitToHistoricalEvent(
        ElementLocation elementLocation,
        SearchHit hit,
        HistoricalEventsFetchHints fetchHints,
        HistoricalEventState state
    ) {
        Mutation m = ProtobufUtils.mutationFromField(hit.getField(FieldNames.MUTATION_DATA));
        switch (m.getMutationCase()) {
            case MUTATION_NOT_SET:
                throw new VertexiumException("not implemented");

            case SET_PROPERTY_MUTATION:
                return setPropertyMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getSetPropertyMutation(),
                    fetchHints,
                    state.previousValues
                );

            case ALTER_EDGE_LABEL_MUTATION:
                HistoricalAlterEdgeLabelEvent alterEdgeLabelEvent = alterEdgeLabelMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getAlterEdgeLabelMutation(),
                    fetchHints
                );
                state.edgeLabel = m.getAlterEdgeLabelMutation().getNewEdgeLabel();
                return alterEdgeLabelEvent;

            case PROPERTY_DELETE_MUTATION:
                return propertyDeleteMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getPropertyDeleteMutation(),
                    fetchHints
                );

            case PROPERTY_SOFT_DELETE_MUTATION:
                return propertySoftDeleteMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getPropertySoftDeleteMutation(),
                    fetchHints
                );

            case MARK_HIDDEN_MUTATION:
                return markHiddenMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getMarkHiddenMutation(),
                    fetchHints
                );

            case MARK_VISIBLE_MUTATION:
                return markVisibleMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getMarkVisibleMutation(),
                    fetchHints
                );

            case MARK_PROPERTY_HIDDEN_MUTATION:
                return markPropertyHiddenMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getMarkPropertyHiddenMutation(),
                    fetchHints
                );

            case MARK_PROPERTY_VISIBLE_MUTATION:
                return markPropertyVisibleMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getMarkPropertyVisibleMutation(),
                    fetchHints
                );

            case ALTER_ELEMENT_VISIBILITY_MUTATION:
                return alterElementVisibilityMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getAlterElementVisibilityMutation(),
                    fetchHints,
                    state
                );

            case ADDITIONAL_VISIBILITY_ADD_MUTATION:
                throw new VertexiumException("not implemented");

            case ADDITIONAL_VISIBILITY_DELETE_MUTATION:
                throw new VertexiumException("not implemented");

            case ALTER_PROPERTY_VISIBILITY_MUTATION:
                return alterPropertyVisibilityMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getAlterPropertyVisibilityMutation(),
                    fetchHints
                );

            case SET_PROPERTY_METADATA_MUTATION:
                return setPropertyMetadataMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getSetPropertyMetadataMutation(),
                    fetchHints
                );

            case DELETE_MUTATION:
                HistoricalEvent deleteMutation = deleteMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getDeleteMutation(),
                    fetchHints,
                    state.elementVisibility,
                    state.edgeLabel
                );
                state.lastAddElementEvent = null;
                return deleteMutation;

            case UPDATE_VERTEX_MUTATION:
                if (!isElementUpdateChange(state.lastAddElementEvent, m.getUpdateVertexMutation())) {
                    return null;
                }
                HistoricalAddVertexEvent addVertexEvent = updateVertexMutationToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getUpdateVertexMutation(),
                    fetchHints
                );
                state.lastAddElementEvent = addVertexEvent;
                return addVertexEvent;

            case UPDATE_EDGE_MUTATION:
                if (!isElementUpdateChange((HistoricalAddEdgeEvent) state.lastAddElementEvent, m.getUpdateEdgeMutation())) {
                    return null;
                }
                HistoricalAddEdgeEvent addEdgeEvent = updateEdgeMutationToHistoricalEvent(
                    (EdgeElementLocation) elementLocation,
                    m.getTimestamp(),
                    m.getUpdateEdgeMutation(),
                    fetchHints
                );
                state.edgeLabel = m.getUpdateEdgeMutation().getLabel();
                state.lastAddElementEvent = addEdgeEvent;
                return addEdgeEvent;

            case ADD_EDGE_TO_VERTEX_MUTATION:
                HistoricalAddEdgeToVertexEvent addEdgeToVertexToHistoricalEvent = addEdgeToVertexToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getAddEdgeToVertexMutation(),
                    fetchHints
                );
                HistoricalAddEdgeToVertexEvent previousAddEdgeToVertexToHistoricalEvent
                    = state.lastAddEdgeToVertexEvent.get(addEdgeToVertexToHistoricalEvent.getEdgeId());
                if (!isAddEdgeToVertexEventChange(addEdgeToVertexToHistoricalEvent, previousAddEdgeToVertexToHistoricalEvent)) {
                    return null;
                }
                state.lastAddEdgeToVertexEvent.put(
                    addEdgeToVertexToHistoricalEvent.getEdgeId(),
                    addEdgeToVertexToHistoricalEvent
                );
                return addEdgeToVertexToHistoricalEvent;

            case DELETE_EDGE_TO_VERTEX_MUTATION:
                return deleteEdgeToVertexToHistoricalEvent(
                    elementLocation,
                    m.getTimestamp(),
                    m.getDeleteEdgeToVertexMutation(),
                    fetchHints
                );

            default:
                throw new VertexiumException("unhandled mutation type: " + m.getMutationCase());
        }
    }

    private HistoricalSoftDeleteEdgeToVertexEvent deleteEdgeToVertexToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        DeleteEdgeToVertexMutation deleteEdgeToVertexMutation,
        HistoricalEventsFetchHints fetchHints
    ) {
        if (deleteEdgeToVertexMutation.getSoftDelete()) {
            return new HistoricalSoftDeleteEdgeToVertexEvent(
                elementLocation.getId(),
                deleteEdgeToVertexMutation.getEdgeId(),
                deleteEdgeToVertexMutation.getDirectionOut() ? Direction.OUT : Direction.IN,
                deleteEdgeToVertexMutation.getEdgeLabel(),
                deleteEdgeToVertexMutation.getOtherVertexId(),
                new Visibility(deleteEdgeToVertexMutation.getEdgeVisibility()),
                HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
                scriptService.valueToJavaObject(deleteEdgeToVertexMutation.getEventData()),
                fetchHints
            );
        }
        return null;
    }

    private boolean isAddEdgeToVertexEventChange(
        HistoricalAddEdgeToVertexEvent event,
        HistoricalAddEdgeToVertexEvent previousEvent
    ) {
        if (previousEvent == null) {
            return true;
        }
        if (!previousEvent.getEdgeLabel().equals(event.getEdgeLabel())) {
            return true;
        }
        if (!previousEvent.getEdgeVisibility().equals(event.getEdgeVisibility())) {
            return true;
        }
        return false;
    }

    private HistoricalAddEdgeToVertexEvent addEdgeToVertexToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        AddEdgeToVertexMutation addEdgeToVertexMutation,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAddEdgeToVertexEvent(
            elementLocation.getId(),
            addEdgeToVertexMutation.getEdgeId(),
            addEdgeToVertexMutation.getDirectionOut() ? Direction.OUT : Direction.IN,
            addEdgeToVertexMutation.getEdgeLabel(),
            addEdgeToVertexMutation.getOtherVertexId(),
            new Visibility(addEdgeToVertexMutation.getEdgeVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            fetchHints
        );
    }

    private HistoricalAlterEdgeLabelEvent alterEdgeLabelMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        AlterEdgeLabelMutation alterEdgeLabelMutation,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAlterEdgeLabelEvent(
            elementLocation.getId(),
            alterEdgeLabelMutation.getNewEdgeLabel(),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            fetchHints
        );
    }

    private boolean isElementUpdateChange(
        HistoricalAddElementEvent lastAddElementEvent,
        UpdateVertexMutation updateVertexMutation
    ) {
        if (lastAddElementEvent == null) {
            return true;
        }
        if (!lastAddElementEvent.getElementId().equals(updateVertexMutation.getId())) {
            return true;
        }
        if (!lastAddElementEvent.getVisibility().equals(new Visibility(updateVertexMutation.getVisibility()))) {
            return true;
        }
        return false;
    }

    private boolean isElementUpdateChange(
        HistoricalAddEdgeEvent lastAddElementEvent,
        UpdateEdgeMutation updateEdgeMutation
    ) {
        if (lastAddElementEvent == null) {
            return true;
        }
        if (!lastAddElementEvent.getElementId().equals(updateEdgeMutation.getId())) {
            return true;
        }
        if (!lastAddElementEvent.getVisibility().equals(new Visibility(updateEdgeMutation.getVisibility()))) {
            return true;
        }
        return false;
    }

    private HistoricalAlterElementVisibilityEvent alterElementVisibilityMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        AlterElementVisibilityMutation m,
        HistoricalEventsFetchHints fetchHints,
        HistoricalEventState state
    ) {
        Visibility oldVisibility = state.elementVisibility;
        Visibility newVisibility = new Visibility(m.getVisibility());
        state.elementVisibility = newVisibility;
        if (elementLocation.getElementType() == ElementType.VERTEX) {
            return new HistoricalAlterVertexVisibilityEvent(
                elementLocation.getId(),
                oldVisibility,
                newVisibility,
                HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
                scriptService.valueToJavaObject(m.getEventData()),
                fetchHints
            );
        } else if (elementLocation.getElementType() == ElementType.EDGE) {
            EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
            return new HistoricalAlterEdgeVisibilityEvent(
                edgeElementLocation.getId(),
                edgeElementLocation.getVertexId(Direction.OUT),
                edgeElementLocation.getVertexId(Direction.IN),
                state.edgeLabel,
                oldVisibility,
                newVisibility,
                HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
                scriptService.valueToJavaObject(m.getEventData()),
                fetchHints
            );
        } else {
            throw new VertexiumException("Unhandled element type: " + elementLocation.getElementType());
        }
    }

    private HistoricalMarkVisibleEvent markVisibleMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        MarkVisibleMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkVisibleEvent(
            elementLocation.getElementType(),
            elementLocation.getId(),
            new Visibility(m.getVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            scriptService.valueToJavaObject(m.getEventData()),
            fetchHints
        );
    }

    private HistoricalMarkHiddenEvent markHiddenMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        MarkHiddenMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkHiddenEvent(
            elementLocation.getElementType(),
            elementLocation.getId(),
            new Visibility(m.getVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            scriptService.valueToJavaObject(m.getEventData()),
            fetchHints
        );
    }

    private HistoricalEvent deleteMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        DeleteMutation m,
        HistoricalEventsFetchHints fetchHints,
        Visibility elementVisibility,
        String edgeLabel
    ) {
        if (m.hasSoftDelete() && m.getSoftDelete()) {
            if (elementLocation.getElementType() == ElementType.VERTEX) {
                return new HistoricalSoftDeleteVertexEvent(
                    elementLocation.getId(),
                    HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
                    scriptService.valueToJavaObject(m.getEventData()),
                    fetchHints
                );
            } else if (elementLocation.getElementType() == ElementType.EDGE) {
                EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
                return new HistoricalSoftDeleteEdgeEvent(
                    edgeElementLocation.getId(),
                    edgeElementLocation.getVertexId(Direction.OUT),
                    edgeElementLocation.getVertexId(Direction.IN),
                    edgeLabel,
                    HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
                    scriptService.valueToJavaObject(m.getEventData()),
                    fetchHints
                );
            } else {
                throw new VertexiumException("Unhandled element type: " + elementLocation.getElementType());
            }
        } else {
            if (elementLocation.getElementType() == ElementType.VERTEX) {
                return new HistoricalDeleteVertexEvent(
                    elementLocation.getId(),
                    elementVisibility,
                    HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
                    fetchHints
                );
            } else if (elementLocation.getElementType() == ElementType.EDGE) {
                EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
                return new HistoricalDeleteEdgeEvent(
                    edgeElementLocation.getId(),
                    edgeElementLocation.getVertexId(Direction.OUT),
                    edgeElementLocation.getVertexId(Direction.IN),
                    edgeLabel,
                    elementVisibility,
                    HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
                    fetchHints
                );
            } else {
                throw new VertexiumException("Unhandled element type: " + elementLocation.getElementType());
            }
        }
    }

    private HistoricalSoftDeletePropertyEvent propertySoftDeleteMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        PropertySoftDeleteMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalSoftDeletePropertyEvent(
            elementLocation.getElementType(),
            elementLocation.getId(),
            m.getKey(),
            m.getName(),
            new Visibility(m.getVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            scriptService.valueToJavaObject(m.getEventData()),
            fetchHints
        );
    }

    private HistoricalEvent propertyDeleteMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        PropertyDeleteMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        throw new VertexiumException("not implemented: " + m);
    }

    private HistoricalAlterPropertyVisibilityEvent alterPropertyVisibilityMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        AlterPropertyVisibilityMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAlterPropertyVisibilityEvent(
            elementLocation.getElementType(),
            elementLocation.getId(),
            m.getKey(),
            m.getName(),
            m.hasOldVisibility() ? new Visibility(m.getOldVisibility()) : null,
            new Visibility(m.getNewVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            scriptService.valueToJavaObject(m.getEventData()),
            fetchHints
        );
    }

    private HistoricalEvent setPropertyMetadataMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        SetPropertyMetadataMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        // no historical event for this mutation
        return null;
    }

    private HistoricalMarkPropertyVisibleEvent markPropertyVisibleMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        MarkPropertyVisibleMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkPropertyVisibleEvent(
            elementLocation.getElementType(),
            elementLocation.getId(),
            m.getKey(),
            m.getName(),
            new Visibility(m.getPropertyVisibility()),
            new Visibility(m.getVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            scriptService.valueToJavaObject(m.getEventData()),
            fetchHints
        );
    }

    private HistoricalMarkPropertyHiddenEvent markPropertyHiddenMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        MarkPropertyHiddenMutation m,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalMarkPropertyHiddenEvent(
            elementLocation.getElementType(),
            elementLocation.getId(),
            m.getKey(),
            m.getName(),
            new Visibility(m.getPropertyVisibility()),
            new Visibility(m.getVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            scriptService.valueToJavaObject(m.getEventData()),
            fetchHints
        );
    }

    private HistoricalAddEdgeEvent updateEdgeMutationToHistoricalEvent(
        EdgeElementLocation elementLocation,
        long timestamp,
        UpdateEdgeMutation updateEdgeMutation,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAddEdgeEvent(
            updateEdgeMutation.getId(),
            updateEdgeMutation.getOutVertexId(),
            updateEdgeMutation.getInVertexId(),
            updateEdgeMutation.getLabel(),
            new Visibility(updateEdgeMutation.getVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            fetchHints
        );
    }

    private HistoricalAddVertexEvent updateVertexMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        UpdateVertexMutation updateVertexMutation,
        HistoricalEventsFetchHints fetchHints
    ) {
        return new HistoricalAddVertexEvent(
            elementLocation.getId(),
            new Visibility(updateVertexMutation.getVisibility()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            fetchHints
        );
    }

    private HistoricalAddPropertyEvent setPropertyMutationToHistoricalEvent(
        ElementLocation elementLocation,
        long timestamp,
        SetPropertyMutation m,
        HistoricalEventsFetchHints fetchHints,
        Map<PropertyLocation, Object> previousValues
    ) {
        PropertyLocation propertyLocation = new PropertyLocation(
            elementLocation,
            m.getKey(),
            m.getName(),
            new Visibility(m.getVisibility())
        );
        Object value = fetchHints.isIncludePreviousPropertyValues() || fetchHints.isIncludePropertyValues()
            ? scriptService.valueToJavaObject(m.getValue())
            : null;
        Object previousValue = fetchHints.isIncludePreviousPropertyValues() ? previousValues.get(propertyLocation) : null;
        if (fetchHints.isIncludePreviousPropertyValues()) {
            previousValues.put(propertyLocation, value);
        }
        return new HistoricalAddPropertyEvent(
            propertyLocation.getElementType(),
            propertyLocation.getId(),
            propertyLocation.getPropertyKey(),
            propertyLocation.getPropertyName(),
            propertyLocation.getPropertyVisibility(),
            previousValue,
            value,
            scriptService.metadataListToMetadata(m.getMetadataList()),
            HistoricalEvent.zonedDateTimeFromTimestamp(timestamp),
            fetchHints
        );
    }

    public Stream<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        // TODO could probably do this in one query
        return stream(ids)
            .map(id -> getVertex(id, fetchHints, endTime, user))
            .filter(Objects::nonNull);
    }

    public Stream<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        // TODO could probably do this in one query
        return stream(ids)
            .map(id -> getEdge(id, fetchHints, endTime, user))
            .filter(Objects::nonNull);
    }

    private static class HistoricalEventState {
        HistoricalAddElementEvent lastAddElementEvent;
        Visibility elementVisibility;
        String edgeLabel;
        Map<PropertyLocation, Object> previousValues = new HashMap<>();
        Map<String, HistoricalAddEdgeToVertexEvent> lastAddEdgeToVertexEvent = new HashMap<>();
    }
}
