package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.vertexium.*;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;
import org.vertexium.mutation.*;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoShape;
import org.vertexium.util.ConvertingIterable;

import java.util.*;
import java.util.stream.Collectors;

import static org.vertexium.elasticsearch5.Elasticsearch5SearchIndex.*;
import static org.vertexium.util.StreamUtils.stream;

class AddOrUpdateService {
    private final Graph graph;
    private final Elasticsearch5SearchIndex searchIndex;
    private final Client client;
    private final ElasticsearchSearchIndexConfiguration config;
    private final IndexService indexService;
    private final IdStrategy idStrategy;
    private final PropertyNameService propertyNameService;
    private final FlushObjectQueue flushObjectQueue;

    public AddOrUpdateService(
        Graph graph,
        Elasticsearch5SearchIndex searchIndex,
        Client client,
        ElasticsearchSearchIndexConfiguration config,
        IndexService indexService,
        IdStrategy idStrategy,
        PropertyNameService propertyNameService,
        FlushObjectQueue flushObjectQueue
    ) {
        this.graph = graph;
        this.searchIndex = searchIndex;
        this.client = client;
        this.config = config;
        this.indexService = indexService;
        this.idStrategy = idStrategy;
        this.propertyNameService = propertyNameService;
        this.flushObjectQueue = flushObjectQueue;
    }

    public <TElement extends Element> void addOrUpdateElement(ElementMutation<TElement> elementMutation) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("updateElement: %s", elementMutation.getId());
        }

        if (!config.isIndexEdges() && elementMutation.getElementType() == ElementType.EDGE) {
            return;
        }

        while (flushObjectQueue.containsElementId(elementMutation.getId())) {
            flushObjectQueue.flush();
        }

        UpdateRequestBuilder updateRequestBuilder = prepareUpdateForMutation(graph, elementMutation);

        if (updateRequestBuilder != null) {
            IndexInfo indexInfo = addMutationPropertiesToIndex(graph, elementMutation);
            indexService.pushChange(indexInfo.getIndexName());
            searchIndex.addActionRequestBuilderForFlush(elementMutation, updateRequestBuilder);

            if (elementMutation instanceof ExistingElementMutation) {
                ExistingElementMutation<TElement> existingElementMutation = (ExistingElementMutation<TElement>) elementMutation;
                TElement element = existingElementMutation.getElement();
                if (existingElementMutation.getNewElementVisibility() != null && element.getFetchHints().isIncludeExtendedDataTableNames()) {
                    ImmutableSet<String> extendedDataTableNames = element.getExtendedDataTableNames();
                    if (extendedDataTableNames != null && !extendedDataTableNames.isEmpty()) {
                        extendedDataTableNames.forEach(tableName ->
                            alterExtendedDataElementTypeVisibility(
                                graph,
                                elementMutation,
                                element.getExtendedData(tableName),
                                existingElementMutation.getOldElementVisibility(),
                                existingElementMutation.getNewElementVisibility()
                            ));
                    }
                }
            }

            if (config.isAutoFlush()) {
                searchIndex.flush(graph);
            }
        }
    }

    private <TElement extends Element> UpdateRequestBuilder prepareUpdateForMutation(
        Graph graph,
        ElementMutation<TElement> mutation
    ) {
        Map<String, String> fieldVisibilityChanges = getFieldVisibilityChanges(graph, mutation);
        List<String> fieldsToRemove = getFieldsToRemove(graph, mutation);
        Map<String, Object> fieldsToSet = getFieldsToSet(graph, mutation);
        Set<String> additionalVisibilities = getAdditionalVisibilities(mutation);
        Set<String> additionalVisibilitiesToDelete = getAdditionalVisibilitiesToDelete(mutation);
        searchIndex.ensureAdditionalVisibilitiesDefined(additionalVisibilities);

        String documentId = idStrategy.createElementDocId(mutation);
        String indexName = indexService.getIndexName(mutation);
        IndexInfo indexInfo = indexService.ensureIndexCreatedAndInitialized(indexName);
        return searchIndex.prepareUpdateFieldsOnDocument(
            indexInfo.getIndexName(),
            documentId,
            mutation,
            fieldsToSet,
            fieldsToRemove,
            fieldVisibilityChanges,
            additionalVisibilities,
            additionalVisibilitiesToDelete
        );
    }

    private <TElement extends Element> Map<String, String> getFieldVisibilityChanges(Graph graph, ElementMutation<TElement> mutation) {
        Map<String, String> fieldVisibilityChanges = new HashMap<>();

        stream(mutation.getAlterPropertyVisibilities())
            .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
            .forEach(p -> {
                String oldFieldName = propertyNameService.addVisibilityToPropertyName(graph, p.getName(), p.getExistingVisibility());
                String newFieldName = propertyNameService.addVisibilityToPropertyName(graph, p.getName(), p.getVisibility());
                fieldVisibilityChanges.put(oldFieldName, newFieldName);

                PropertyDefinition propertyDefinition = indexService.getPropertyDefinition(graph, p.getName());
                if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
                    fieldVisibilityChanges.put(oldFieldName + GEO_PROPERTY_NAME_SUFFIX, newFieldName + GEO_PROPERTY_NAME_SUFFIX);

                    if (GeoPoint.class.isAssignableFrom(propertyDefinition.getDataType())) {
                        fieldVisibilityChanges.put(oldFieldName + GEO_POINT_PROPERTY_NAME_SUFFIX, newFieldName + GEO_POINT_PROPERTY_NAME_SUFFIX);
                    }
                }
            });

        if (mutation instanceof ExistingElementMutation) {
            ExistingElementMutation<TElement> existingElementMutation = (ExistingElementMutation<TElement>) mutation;
            if (existingElementMutation.getNewElementVisibility() != null) {
                String oldFieldName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, existingElementMutation.getOldElementVisibility());
                String newFieldName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, existingElementMutation.getNewElementVisibility());
                fieldVisibilityChanges.put(oldFieldName, newFieldName);
            }
        }

        return fieldVisibilityChanges;
    }


    private <TElement extends Element> IndexInfo addMutationPropertiesToIndex(Graph graph, ElementMutation<TElement> mutation) {
        IndexInfo indexInfo = indexService.addPropertiesToIndex(graph, mutation, mutation.getProperties());
        stream(mutation.getAlterPropertyVisibilities())
            .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
            .forEach(p -> {
                PropertyDefinition propertyDefinition = indexService.getPropertyDefinition(graph, p.getName());
                if (propertyDefinition != null) {
                    try {
                        indexService.addPropertyDefinitionToIndex(graph, indexInfo, p.getName(), p.getVisibility(), propertyDefinition);
                    } catch (Exception e) {
                        throw new VertexiumException("Unable to add property to index: " + p, e);
                    }
                }
            });

        if (mutation instanceof ExistingElementMutation) {
            ExistingElementMutation<TElement> existingElementMutation = (ExistingElementMutation<TElement>) mutation;
            TElement element = existingElementMutation.getElement();
            if (existingElementMutation.getNewElementVisibility() != null) {
                try {
                    String newFieldName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, existingElementMutation.getNewElementVisibility());
                    indexService.addPropertyToIndex(graph, indexInfo, newFieldName, element.getVisibility(), String.class, false, false, false);
                } catch (Exception e) {
                    throw new VertexiumException("Unable to add new element type visibility to index", e);
                }
            }
        }
        return indexInfo;
    }

    private <TElement extends Element> Map<String, Object> getFieldsToSet(
        Graph graph,
        ElementMutation<TElement> mutation
    ) {
        Map<String, Object> fieldsToSet = new HashMap<>();

        if (mutation instanceof ExistingElementMutation) {
            TElement element = ((ExistingElementMutation<TElement>) mutation).getElement();
            mutation.getProperties().forEach(p ->
                indexService.addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));
            mutation.getPropertyDeletes().forEach(p ->
                indexService.addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));
            mutation.getPropertySoftDeletes().forEach(p ->
                indexService.addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));
        } else {
            fieldsToSet.putAll(indexService.getPropertiesAsFields(graph, mutation.getProperties()));
            // TODO deletes?
            // TODO soft deletes?
        }

        return fieldsToSet;
    }

    private <TElement extends Element> Set<String> getAdditionalVisibilities(ElementMutation<TElement> mutation) {
        Set<String> results = new HashSet<>();
        for (AdditionalVisibilityAddMutation additionalVisibility : mutation.getAdditionalVisibilities()) {
            results.add(additionalVisibility.getAdditionalVisibility());
        }
        return results;
    }

    private <TElement extends Element> Set<String> getAdditionalVisibilitiesToDelete(ElementMutation<TElement> mutation) {
        Set<String> results = new HashSet<>();
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : mutation.getAdditionalVisibilityDeletes()) {
            results.add(additionalVisibilityDelete.getAdditionalVisibility());
        }
        return results;
    }

    private <TElement extends Element> List<String> getFieldsToRemove(Graph graph, ElementMutation<TElement> mutation) {
        List<String> fieldsToRemove = new ArrayList<>();
        mutation.getPropertyDeletes().forEach(p -> {
            String propertyName = propertyNameService.addVisibilityToPropertyName(graph, p.getName(), p.getVisibility());
            fieldsToRemove.add(propertyName);

            PropertyDefinition propertyDefinition = indexService.getPropertyDefinition(graph, p.getName());
            if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
                fieldsToRemove.add(propertyName + GEO_PROPERTY_NAME_SUFFIX);

                if (GeoPoint.class.isAssignableFrom(propertyDefinition.getDataType())) {
                    fieldsToRemove.add(propertyName + GEO_POINT_PROPERTY_NAME_SUFFIX);
                }
            }
        });
        mutation.getPropertySoftDeletes().forEach(p ->
            fieldsToRemove.add(propertyNameService.addVisibilityToPropertyName(graph, p.getName(), p.getVisibility())));
        return fieldsToRemove;
    }

    public <T extends Element> void alterExtendedDataElementTypeVisibility(
        Graph graph,
        ElementMutation<T> elementMutation,
        Iterable<ExtendedDataRow> rows,
        Visibility oldVisibility,
        Visibility newVisibility
    ) {
        searchIndex.bulkUpdate(graph, new ConvertingIterable<ExtendedDataRow, UpdateRequest>(rows) {
            @Override
            protected UpdateRequest convert(ExtendedDataRow row) {
                String tableName = (String) row.getPropertyValue(ExtendedDataRow.TABLE_NAME);
                String rowId = (String) row.getPropertyValue(ExtendedDataRow.ROW_ID);
                String extendedDataDocId = idStrategy.createExtendedDataDocId(elementMutation, tableName, rowId);

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

                IndexInfo indexInfo = indexService.addExtendedDataColumnsToIndex(graph, elementMutation, tableName, rowId, columns);
                indexService.pushChange(indexInfo.getIndexName());

                String oldElementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, oldVisibility);
                String newElementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, newVisibility);
                Map<String, String> fieldsToRename = Collections.singletonMap(oldElementTypeVisibilityPropertyName, newElementTypeVisibilityPropertyName);

                return client
                    .prepareUpdate(indexInfo.getIndexName(), idStrategy.getType(), extendedDataDocId)
                    .setScript(new Script(
                        ScriptType.STORED,
                        "painless",
                        "updateFieldsOnDocumentScript",
                        ImmutableMap.of(
                            "fieldsToSet", Collections.emptyMap(),
                            "fieldsToRemove", Collections.emptyList(),
                            "fieldsToRename", fieldsToRename,
                            "additionalVisibilities", Collections.emptyList(),
                            "additionalVisibilitiesToDelete", Collections.emptyList()
                        )
                    ))
                    .setRetryOnConflict(FlushObjectQueue.MAX_RETRIES)
                    .request();
            }
        });
    }
}
