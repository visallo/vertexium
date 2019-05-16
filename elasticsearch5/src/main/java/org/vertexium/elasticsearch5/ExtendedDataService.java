package org.vertexium.elasticsearch5;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.vertexium.*;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityAddMutation;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityDeleteMutation;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.util.ConvertingIterable;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.vertexium.elasticsearch5.Elasticsearch5SearchIndex.*;
import static org.vertexium.util.StreamUtils.stream;

public class ExtendedDataService {
    private final Elasticsearch5SearchIndex searchIndex;
    private final Client client;
    private final ElasticsearchSearchIndexConfiguration config;
    private final IdStrategy idStrategy;
    private final PropertyNameService propertyNameService;
    private final IndexService indexService;

    public ExtendedDataService(
        Elasticsearch5SearchIndex searchIndex,
        Client client,
        ElasticsearchSearchIndexConfiguration config,
        IdStrategy idStrategy,
        PropertyNameService propertyNameService,
        IndexService indexService
    ) {
        this.searchIndex = searchIndex;
        this.client = client;
        this.config = config;
        this.idStrategy = idStrategy;
        this.propertyNameService = propertyNameService;
        this.indexService = indexService;
    }

    void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String rowId,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes
    ) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElementExtendedData: %s:%s:%s", elementLocation.getId(), tableName, rowId);
        }

        UpdateRequestBuilder updateRequestBuilder = prepareUpdate(
            graph,
            elementLocation,
            tableName,
            rowId,
            extendedData,
            additionalExtendedDataVisibilities,
            additionalExtendedDataVisibilityDeletes
        );
        searchIndex.addActionRequestBuilderForFlush(elementLocation, tableName, rowId, updateRequestBuilder);

        if (config.isAutoFlush()) {
            searchIndex.flush(graph);
        }
    }

    private UpdateRequestBuilder prepareUpdate(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String rowId,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes
    ) {
        try {
            IndexInfo indexInfo = indexService.addExtendedDataColumnsToIndex(graph, elementLocation, tableName, rowId, extendedData);
            String extendedDataDocId = idStrategy.createExtendedDataDocId(elementLocation, tableName, rowId);
            indexService.pushChange(indexInfo.getIndexName());

            Map<String, Object> fieldsToSet =
                indexService.getExtendedDataColumnsAsFields(graph, extendedData).entrySet().stream()
                    .collect(Collectors.toMap(e -> propertyNameService.replaceFieldnameDots(e.getKey()), Map.Entry::getValue));

            XContentBuilder source = buildJsonContentForExtendedDataUpsert(graph, elementLocation, tableName, rowId);
            if (MUTATION_LOGGER.isTraceEnabled()) {
                String fieldsDebug = Joiner.on(", ").withKeyValueSeparator(": ").join(fieldsToSet);
                MUTATION_LOGGER.trace(
                    "addElementExtendedData json: %s:%s:%s: %s {%s}",
                    elementLocation.getId(),
                    tableName,
                    rowId,
                    source.string(),
                    fieldsDebug
                );
            }

            List<String> additionalVisibilities = additionalExtendedDataVisibilities == null
                ? Collections.emptyList()
                : stream(additionalExtendedDataVisibilities).map(AdditionalExtendedDataVisibilityAddMutation::getAdditionalVisibility).collect(Collectors.toList());
            List<String> additionalVisibilitiesToDelete = additionalExtendedDataVisibilityDeletes == null
                ? Collections.emptyList()
                : stream(additionalExtendedDataVisibilityDeletes).map(AdditionalExtendedDataVisibilityDeleteMutation::getAdditionalVisibility).collect(Collectors.toList());
            searchIndex.ensureAdditionalVisibilitiesDefined(additionalVisibilities);

            return client
                .prepareUpdate(indexInfo.getIndexName(), idStrategy.getType(), extendedDataDocId)
                .setScriptedUpsert(true)
                .setUpsert(source)
                .setScript(new Script(
                    ScriptType.STORED,
                    "painless",
                    "updateFieldsOnDocumentScript",
                    ImmutableMap.of(
                        "fieldsToSet", fieldsToSet,
                        "fieldsToRemove", Collections.emptyList(),
                        "fieldsToRename", Collections.emptyMap(),
                        "additionalVisibilities", additionalVisibilities,
                        "additionalVisibilitiesToDelete", additionalVisibilitiesToDelete
                    )))
                .setRetryOnConflict(FlushObjectQueue.MAX_RETRIES);
        } catch (IOException e) {
            throw new VertexiumException("Could not add element extended data", e);
        }
    }

    public void addExtendedData(Graph graph, ElementLocation elementLocation, Iterable<ExtendedDataRow> extendedDatas) {
        Map<ElementType, Map<String, List<ExtendedDataRow>>> rowsByElementTypeAndId = mapExtendedDatasByElementTypeByElementId(extendedDatas);
        rowsByElementTypeAndId.forEach((elementType, elements) -> {
            elements.forEach((elementId, rows) -> {
                searchIndex.bulkUpdate(graph, new ConvertingIterable<ExtendedDataRow, UpdateRequest>(rows) {
                    @Override
                    protected UpdateRequest convert(ExtendedDataRow row) {
                        String tableName = (String) row.getPropertyValue(ExtendedDataRow.TABLE_NAME);
                        String rowId = (String) row.getPropertyValue(ExtendedDataRow.ROW_ID);
                        List<ExtendedDataMutation> columns = stream(row.getProperties())
                            .map(property -> new ExtendedDataMutation(
                                tableName,
                                rowId,
                                property.getName(),
                                property.getKey(),
                                property.getValue(),
                                property.getTimestamp(),
                                property.getVisibility()
                            )).collect(Collectors.toList());
                        return prepareUpdate(
                            graph,
                            elementLocation,
                            tableName,
                            rowId,
                            columns,
                            Collections.emptyList(),
                            Collections.emptyList()
                        ).request();
                    }
                });
            });
        });
    }

    private Map<ElementType, Map<String, List<ExtendedDataRow>>> mapExtendedDatasByElementTypeByElementId(Iterable<ExtendedDataRow> extendedData) {
        Map<ElementType, Map<String, List<ExtendedDataRow>>> rowsByElementTypeByElementId = new HashMap<>();
        extendedData.forEach(row -> {
            ExtendedDataRowId rowId = row.getId();
            Map<String, List<ExtendedDataRow>> elementTypeData = rowsByElementTypeByElementId.computeIfAbsent(rowId.getElementType(), key -> new HashMap<>());
            List<ExtendedDataRow> elementExtendedData = elementTypeData.computeIfAbsent(rowId.getElementId(), key -> new ArrayList<>());
            elementExtendedData.add(row);
        });
        return rowsByElementTypeByElementId;
    }

    private XContentBuilder buildJsonContentForExtendedDataUpsert(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String rowId
    ) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder().startObject();

        String elementTypeString = ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElement(
            elementLocation.getElementType()
        ).getKey();
        jsonBuilder.field(ELEMENT_ID_FIELD_NAME, elementLocation.getId());
        jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, elementTypeString);
        String elementTypeVisibilityPropertyName = indexService.addElementTypeVisibilityPropertyToExtendedDataIndex(
            graph,
            elementLocation,
            tableName,
            rowId
        );
        jsonBuilder.field(elementTypeVisibilityPropertyName, elementTypeString);
        jsonBuilder.field(EXTENDED_DATA_TABLE_NAME_FIELD_NAME, tableName);
        jsonBuilder.field(EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME, rowId);
        if (elementLocation.getElementType() == ElementType.EDGE) {
            if (!(elementLocation instanceof EdgeElementLocation)) {
                throw new VertexiumException(String.format(
                    "element location (%s) has type edge but does not implement %s",
                    elementLocation.getClass().getName(),
                    EdgeElementLocation.class.getName()
                ));
            }
            EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
            jsonBuilder.field(IN_VERTEX_ID_FIELD_NAME, edgeElementLocation.getVertexId(Direction.IN));
            jsonBuilder.field(OUT_VERTEX_ID_FIELD_NAME, edgeElementLocation.getVertexId(Direction.OUT));
            jsonBuilder.field(EDGE_LABEL_FIELD_NAME, edgeElementLocation.getLabel());
        }

        jsonBuilder.endObject();

        return jsonBuilder;
    }
}
