package org.vertexium.elasticsearch5;

import com.google.common.collect.Lists;
import io.netty.util.internal.ConcurrentSet;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.vertexium.*;
import org.vertexium.elasticsearch5.models.Mutation;
import org.vertexium.elasticsearch5.models.Mutations;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.*;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.IncreasingTime;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.StreamUtils.stream;

public class Elasticsearch5Graph extends GraphBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(Elasticsearch5Graph.class);
    public static final int EXACT_MATCH_IGNORE_ABOVE_LIMIT = 10000;
    public static final String LOWERCASER_NORMALIZER_NAME = "visallo_lowercaser";
    public static final String ADDITIONAL_VISIBILITY_METADATA_PREFIX = "elasticsearch_additionalVisibility_";
    private final Set<Visibility> additionalVisibilitiesCache = new ConcurrentSet<>();

    private final Client client;
    private final Elasticsearch5GraphMetadataStore graphMetadataStore;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final IdStrategy idStrategy;
    private final IndexService indexService;
    private final PropertyNameService propertyNameService;
    private final PropertyNameVisibilitiesStore propertyNameVisibilityStore;
    private final MutationStore mutationStore;
    private final ScriptService scriptService;
    private final StreamingPropertyValueService streamingPropertyValueService;
    private final Set<String> validAuthorizations;
    private final Elasticsearch5GraphExceptionHandler exceptionHandler;
    private final FlushObjectQueue flushObjectQueue;

    public Elasticsearch5Graph(Elasticsearch5GraphConfiguration config) {
        super(config);
        client = new ClientService(config).createClient();
        exceptionHandler = config.getExceptionHandler(this);
        flushObjectQueue = new FlushObjectQueue(this);
        idStrategy = new IdStrategy();
        indexSelectionStrategy = new DefaultIndexSelectionStrategy(config);
        propertyNameVisibilityStore = new PropertyNameVisibilitiesStore(this);
        propertyNameService = new PropertyNameService(
            propertyNameVisibilityStore
        );
        indexService = new IndexService(
            this,
            config,
            propertyNameService,
            indexSelectionStrategy,
            propertyNameVisibilityStore,
            idStrategy
        );
        streamingPropertyValueService = new LocalDiskStreamingPropertyValueService();
        scriptService = new ScriptService(propertyNameService, streamingPropertyValueService);
        graphMetadataStore = new Elasticsearch5GraphMetadataStore(
            this,
            client,
            indexService,
            indexSelectionStrategy,
            idStrategy,
            scriptService
        );
        mutationStore = new MutationStore(this, indexSelectionStrategy, scriptService);
        validAuthorizations = graphMetadataStore.getValidAuthorizations();
    }

    public static Graph create(Elasticsearch5GraphConfiguration configuration) {
        return new Elasticsearch5Graph(configuration);
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        addValidAuthorizations(visibility);
        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                User user = authorizations.getUser();
                saveMutation(this, timestamp, user);
                flush();
                return getVertex(getId(), user);
            }

            @Override
            public String save(User user) {
                saveMutation(this, timestamp, user);
                return getId();
            }
        };
    }

    <T extends Element> void saveMutation(ElementMutation<T> mutation, Long timestamp, User user) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        IndexInfo indexInfo = indexService.addPropertiesToIndex(
            mutation,
            mutation.getProperties(),
            mutation.getAlterPropertyVisibilities(),
            mutation.getMarkHiddenData()
        );
        if (mutation.isDeleteElement() || mutation.getSoftDeleteData() != null) {
            saveDeleteElementPartOfMutation(mutation, timestamp);
        } else {
            saveElementPartOfMutation(mutation, timestamp, indexInfo, user);
            saveExtendedDataPartOfMutation(mutation, timestamp);
        }
    }

    private <T extends Element> void saveElementPartOfMutation(
        ElementMutation<T> mutation,
        long timestamp,
        IndexInfo indexInfo,
        User user
    ) {
        ensureAdditionalVisibilitiesDefinedMutations(mutation.getAdditionalVisibilities());

        if (mutation.isDeleteElement()) {
            return;
        }
        Map<String, Object> scriptParameters = new HashMap<>();
        Mutations mutations = scriptService.mutationToScriptParameter(mutation, timestamp);
        scriptParameters.put(ParameterNames.UPDATE, mutations.toByteArray());

        String documentId = idStrategy.createElementDocId(mutation);
        XContentBuilder jsonBuilder = buildJsonFromMutation(mutation);

        UpdateRequestBuilder updateRequestBuilder = getClient().prepareUpdate()
            .setIndex(indexInfo.getIndexName())
            .setId(documentId)
            .setUpsert(jsonBuilder)
            .setScriptedUpsert(true)
            .setType(idStrategy.getType())
            .setScript(new Script(
                ScriptType.INLINE,
                "native",
                "vertexiumSaveMutation",
                scriptParameters
            ));
        ListenableActionFuture<UpdateResponse> future = updateRequestBuilder
            .execute();
        flushObjectQueue.add(mutation, updateRequestBuilder, future);

        if (mutation.getElementType() == ElementType.VERTEX) {
            List<ElementMutationBase.MarkHiddenData> markHiddenData = toList(mutation.getMarkHiddenData());
            List<ElementMutationBase.MarkVisibleData> markVisibleData = toList(mutation.getMarkVisibleData());
            if (markHiddenData.size() > 0 || markVisibleData.size() > 0) {
                FetchHints edgeFetchHints = new FetchHintsBuilder()
                    .setIncludeHidden(true)
                    .build();
                Stream<Edge> edges = getEdgesForVertex(mutation.getId(), edgeFetchHints, user);
                edges.forEach(edge -> {
                    ExistingEdgeMutation edgeMutation = edge.prepareMutation();
                    for (ElementMutationBase.MarkHiddenData markHidden : markHiddenData) {
                        edgeMutation.markElementHidden(markHidden.getVisibility(), markHidden.getEventData());
                    }
                    for (ElementMutationBase.MarkVisibleData markVisible : markVisibleData) {
                        edgeMutation.markElementVisible(markVisible.getVisibility(), markVisible.getEventData());
                    }
                    edgeMutation.save(user);
                });
            }
        }

        saveMutationsToMutationIndex(mutation, mutations.getMutationsList());
    }

    private void saveMutationsToMutationIndex(ElementLocation elementLocation, List<Mutation> mutations) {
        for (Mutation mutation : mutations) {
            try {
                String mutationIndexName = indexSelectionStrategy.getMutationIndexName(this, mutation);
                indexService.ensureMutationIndexCreatedAndInitialized(mutationIndexName);

                // TODO should we store extended data mutation separately?
                XContentBuilder docBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FieldNames.MUTATION_ELEMENT_TYPE, elementLocation.getElementType().name())
                    .field(FieldNames.MUTATION_ELEMENT_ID, elementLocation.getId())
                    .field(FieldNames.MUTATION_TIMESTAMP, mutation.getTimestamp())
                    .field(FieldNames.MUTATION_TYPE, mutation.getMutationCase().name())
                    .field(FieldNames.MUTATION_DATA, mutation.toByteArray());
                if (elementLocation.getElementType() == ElementType.EDGE) {
                    EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
                    docBuilder = docBuilder
                        .field(FieldNames.MUTATION_OUT_VERTEX_ID, edgeElementLocation.getVertexId(Direction.OUT))
                        .field(FieldNames.MUTATION_IN_VERTEX_ID, edgeElementLocation.getVertexId(Direction.IN))
                        .field(FieldNames.MUTATION_EDGE_LABEL, edgeElementLocation.getLabel());
                }
                XContentBuilder doc = docBuilder
                    .endObject();
                IndexRequestBuilder indexRequest = getClient().prepareIndex() // TODO: should we handle updates?
                    .setIndex(mutationIndexName)
                    .setId(idStrategy.createMutationDocId(elementLocation, mutation))
                    .setSource(doc)
                    .setType(idStrategy.getMutationType());
                ListenableActionFuture<IndexResponse> future = indexRequest.execute();
                flushObjectQueue.add(elementLocation, indexRequest, future);
            } catch (IOException ex) {
                throw new VertexiumException("Could not save mutation: " + mutation, ex);
            }
        }
    }

    private <T extends Element> void saveDeleteElementPartOfMutation(
        ElementMutation<T> mutation,
        long timestamp
    ) {
        String elementTypeString = ElasticsearchDocumentType.fromElementType(mutation.getElementType()).getKey();
        String extDataElementTypeString = ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElementType(
            mutation.getElementType()
        ).getKey();
        QueryBuilder elementTypeQuery = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery(FieldNames.ELEMENT_TYPE, elementTypeString))
            .should(QueryBuilders.termQuery(FieldNames.ELEMENT_TYPE, extDataElementTypeString))
            .minimumShouldMatch(1);
        QueryBuilder deleteElementQuery = QueryBuilders.boolQuery()
            .must(elementTypeQuery)
            .must(QueryBuilders.termQuery(FieldNames.ELEMENT_ID, mutation.getId()));
        QueryBuilder query;
        if (mutation.getElementType() == ElementType.VERTEX) {
            QueryBuilder edgeElementTypeQuery = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery(FieldNames.ELEMENT_TYPE, ElasticsearchDocumentType.EDGE.getKey()))
                .should(QueryBuilders.termQuery(FieldNames.ELEMENT_TYPE, ElasticsearchDocumentType.EDGE_EXTENDED_DATA.getKey()))
                .minimumShouldMatch(1);
            QueryBuilder inOrOutVertexIdQuery = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery(FieldNames.IN_VERTEX_ID, mutation.getId()))
                .should(QueryBuilders.termQuery(FieldNames.OUT_VERTEX_ID, mutation.getId()))
                .minimumShouldMatch(1);
            QueryBuilder deleteEdgesQuery = QueryBuilders.boolQuery()
                .must(edgeElementTypeQuery)
                .must(inOrOutVertexIdQuery);
            query = QueryBuilders.boolQuery()
                .should(deleteElementQuery)
                .should(deleteEdgesQuery)
                .minimumShouldMatch(1);
        } else {
            query = deleteElementQuery;
        }
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(getClient(), DeleteByQueryAction.INSTANCE)
            .filter(query)
            .source(getIndexService().getIndexNames());
        ListenableActionFuture<BulkByScrollResponse> future = deleteByQueryRequestBuilder
            .execute();
        flushObjectQueue.add(mutation, deleteByQueryRequestBuilder, future);

        Mutation deleteMutation = scriptService.deleteMutationToMutation(mutation, timestamp);
        saveMutationsToMutationIndex(mutation, Lists.newArrayList(deleteMutation));
    }

    private <T extends Element> void saveExtendedDataPartOfMutation(ElementMutation<T> mutation, long timestamp) {
        List<ExtendedDataMutation> extendedData = toList(mutation.getExtendedData());
        List<ExtendedDataDeleteMutation> extendedDataDeletes = toList(mutation.getExtendedDataDeletes());
        List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities
            = toList(mutation.getAdditionalExtendedDataVisibilities());
        List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes
            = toList(mutation.getAdditionalExtendedDataVisibilityDeletes());

        Visibility newElementVisibility = null;
        Element element = null;
        if (mutation instanceof ExistingElementMutation) {
            ExistingElementMutation existingElementMutation = (ExistingElementMutation) mutation;
            element = existingElementMutation.getElement();
            if (existingElementMutation.getNewElementVisibility() != null) {
                newElementVisibility = existingElementMutation.getNewElementVisibility();
            }
        }
        if (extendedData.size() > 0
            || extendedDataDeletes.size() > 0
            || additionalExtendedDataVisibilities.size() > 0
            || additionalExtendedDataVisibilityDeletes.size() > 0
            || newElementVisibility != null) {
            ensureAdditionalVisibilitiesDefinedExtendedData(additionalExtendedDataVisibilities);

            Map<String, List<ExtendedDataMutation>> extendedDataByDoc = extendedData.stream()
                .collect(Collectors.groupingBy(ed -> ed.getTableName() + ed.getRow()));
            Map<String, List<ExtendedDataDeleteMutation>> extendedDataDeletesByDoc = extendedDataDeletes.stream()
                .collect(Collectors.groupingBy(ed -> ed.getTableName() + ed.getRow()));
            Map<String, List<AdditionalExtendedDataVisibilityAddMutation>> additionalExtendedDataVisibilitiesByDoc = additionalExtendedDataVisibilities.stream()
                .collect(Collectors.groupingBy(ed -> ed.getTableName() + ed.getRow()));
            Map<String, List<AdditionalExtendedDataVisibilityDeleteMutation>> additionalExtendedDataVisibilityDeletesByDoc = additionalExtendedDataVisibilityDeletes.stream()
                .collect(Collectors.groupingBy(ed -> ed.getTableName() + ed.getRow()));
            Set<String> allDocs = new HashSet<>();
            allDocs.addAll(extendedDataByDoc.keySet());
            allDocs.addAll(extendedDataDeletesByDoc.keySet());
            allDocs.addAll(additionalExtendedDataVisibilitiesByDoc.keySet());
            allDocs.addAll(additionalExtendedDataVisibilityDeletesByDoc.keySet());

            // TODO how do we delete the whole document if all columns are deleted?

            // TODO optimize this to new store all rows in memory
            Map<String, ExtendedDataRow> rowsByDoc = new HashMap<>();
            if (newElementVisibility != null) {
                // TODO make one call instead of needing tables then call per table
                for (String extendedDataTableName : element.getExtendedDataTableNames()) {
                    for (ExtendedDataRow row : element.getExtendedData(extendedDataTableName)) {
                        String doc = extendedDataTableName + row.getId().getRowId();
                        allDocs.add(doc);
                        rowsByDoc.put(doc, row);
                    }
                }
            }

            for (String doc : allDocs) {
                List<ExtendedDataMutation> extendedDataForDoc = extendedDataByDoc.get(doc);
                List<ExtendedDataDeleteMutation> extendedDataDeletesForDoc = extendedDataDeletesByDoc.get(doc);
                List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilitiesForDoc = additionalExtendedDataVisibilitiesByDoc.get(doc);
                List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletesForDoc = additionalExtendedDataVisibilityDeletesByDoc.get(doc);
                String tableName;
                String rowId;
                if (extendedDataForDoc != null && extendedDataForDoc.size() > 0) {
                    tableName = extendedDataForDoc.get(0).getTableName();
                    rowId = extendedDataForDoc.get(0).getRow();
                } else if (extendedDataDeletesForDoc != null && extendedDataDeletesForDoc.size() > 0) {
                    tableName = extendedDataDeletesForDoc.get(0).getTableName();
                    rowId = extendedDataDeletesForDoc.get(0).getRow();
                } else if (additionalExtendedDataVisibilitiesForDoc != null && additionalExtendedDataVisibilitiesForDoc.size() > 0) {
                    tableName = additionalExtendedDataVisibilitiesForDoc.get(0).getTableName();
                    rowId = additionalExtendedDataVisibilitiesForDoc.get(0).getRow();
                } else if (additionalExtendedDataVisibilityDeletesForDoc != null && additionalExtendedDataVisibilityDeletesForDoc.size() > 0) {
                    tableName = additionalExtendedDataVisibilityDeletesForDoc.get(0).getTableName();
                    rowId = additionalExtendedDataVisibilityDeletesForDoc.get(0).getRow();
                } else {
                    ExtendedDataRow row = rowsByDoc.get(doc);
                    if (row != null) {
                        tableName = row.getId().getTableName();
                        rowId = row.getId().getRowId();
                    } else {
                        throw new VertexiumException("invalid state");
                    }
                }

                saveExtendedDataRow(
                    mutation,
                    tableName,
                    rowId,
                    extendedDataForDoc,
                    extendedDataDeletesForDoc,
                    additionalExtendedDataVisibilitiesForDoc,
                    additionalExtendedDataVisibilityDeletesForDoc,
                    newElementVisibility,
                    timestamp
                );
            }
        }
    }

    private void saveExtendedDataRow(
        ElementLocation elementLocation,
        String tableName,
        String rowId,
        List<ExtendedDataMutation> extendedData,
        List<ExtendedDataDeleteMutation> extendedDataDeletes,
        List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        Visibility newElementVisibility,
        long timestamp
    ) {
        IndexInfo indexInfo = indexService.addPropertiesToIndex(elementLocation, tableName, rowId, extendedData);
        String documentId = idStrategy.createExtendedDataDocId(elementLocation, tableName, rowId);
        XContentBuilder jsonBuilder = buildJsonForExtendedData(
            elementLocation,
            tableName,
            rowId
        );

        Map<String, Object> scriptParameters = new HashMap<>();
        Mutations mutations = scriptService.extendedDataToScriptParameters(
            extendedData,
            extendedDataDeletes,
            additionalExtendedDataVisibilities,
            additionalExtendedDataVisibilityDeletes,
            newElementVisibility,
            timestamp
        );
        scriptParameters.put(ParameterNames.UPDATE, mutations.toByteArray());

        UpdateRequestBuilder updateRequestBuilder = getClient().prepareUpdate()
            .setIndex(indexInfo.getIndexName())
            .setId(documentId)
            .setUpsert(jsonBuilder)
            .setScriptedUpsert(true)
            .setType(idStrategy.getType())
            .setScript(new Script(
                ScriptType.INLINE,
                "native",
                "vertexiumSaveExtendedDataMutation",
                scriptParameters
            ));
        ListenableActionFuture<UpdateResponse> future = updateRequestBuilder
            .execute();
        flushObjectQueue.add(elementLocation, updateRequestBuilder, future);

        saveMutationsToMutationIndex(elementLocation, mutations.getMutationsList());
    }

    private XContentBuilder buildJsonForExtendedData(ElementLocation elementLocation, String tableName, String rowId) {
        try {
            XContentBuilder jsonBuilder;
            jsonBuilder = XContentFactory.jsonBuilder().startObject();

            String elementTypeString = ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElementType(
                elementLocation.getElementType()
            ).getKey();
            jsonBuilder.field(FieldNames.ELEMENT_ID, elementLocation.getId());
            jsonBuilder.field(FieldNames.ELEMENT_TYPE, elementTypeString);
            String elementTypeVisibilityPropertyName = indexService.addElementTypeVisibilityPropertyToExtendedDataIndex(
                elementLocation,
                tableName,
                rowId
            );
            jsonBuilder.field(elementTypeVisibilityPropertyName, elementTypeString);
            jsonBuilder.field(FieldNames.EXTENDED_DATA_TABLE_NAME, tableName);
            jsonBuilder.field(FieldNames.EXTENDED_DATA_TABLE_ROW_ID, rowId);
            if (elementLocation.getElementType() == ElementType.EDGE) {
                if (!(elementLocation instanceof EdgeElementLocation)) {
                    throw new VertexiumException(String.format(
                        "element location (%s) has type edge but does not implement %s",
                        elementLocation.getClass().getName(),
                        EdgeElementLocation.class.getName()
                    ));
                }
                EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
                jsonBuilder.field(FieldNames.IN_VERTEX_ID, edgeElementLocation.getVertexId(Direction.IN));
                jsonBuilder.field(FieldNames.OUT_VERTEX_ID, edgeElementLocation.getVertexId(Direction.OUT));
                jsonBuilder.field(FieldNames.EDGE_LABEL, edgeElementLocation.getLabel());
            }

            jsonBuilder.endObject();

            return jsonBuilder;
        } catch (IOException ex) {
            throw new VertexiumException("Could not build document", ex);
        }
    }

    private Stream<Edge> getEdgesForVertex(String vertexId, FetchHints fetchHints, User user) {
        validateAuthorizations(user);
        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations()))
            .has(Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME, vertexId)
            .edges(fetchHints));
    }

    private void validateAuthorizations(User user) {
        for (String auth : user.getAuthorizations()) {
            if (!this.validAuthorizations.contains(auth)) {
                throw new SecurityVertexiumException("Invalid authorizations", user);
            }
        }
    }

    private void addValidAuthorizations(Visibility visibility) {
        addValidAuthorizations(visibility.getAuthorizations());
    }

    private void addValidAuthorizations(String[] auths) {
        for (String authorization : auths) {
            if (!validAuthorizations.contains(authorization)) {
                validAuthorizations.add(authorization);
                graphMetadataStore.addValidAuthorization(authorization);
            }
        }
    }

    private <T extends Element> XContentBuilder buildJsonFromMutation(ElementMutation<T> mutation) {
        try {
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

            String elementTypeVisibilityPropertyName = indexService.addElementTypeVisibilityPropertyToIndex(mutation);

            jsonBuilder.field(FieldNames.ELEMENT_ID, mutation.getId());
            jsonBuilder.field(FieldNames.ELEMENT_VISIBILITY, mutation.getVisibility().getVisibilityString());
            jsonBuilder.field(FieldNames.ELEMENT_TYPE, getElementTypeValueFromElementType(mutation.getElementType()));
            if (mutation.getElementType() == ElementType.VERTEX) {
                jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.VERTEX.getKey());
            } else if (mutation.getElementType() == ElementType.EDGE) {
                EdgeElementLocation edgeElementLocation = (EdgeElementLocation) mutation;
                jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.EDGE.getKey());
                jsonBuilder.field(FieldNames.IN_VERTEX_ID, edgeElementLocation.getVertexId(Direction.IN));
                jsonBuilder.field(FieldNames.OUT_VERTEX_ID, edgeElementLocation.getVertexId(Direction.OUT));
                jsonBuilder.field(FieldNames.EDGE_LABEL, edgeElementLocation.getLabel());
            } else {
                throw new VertexiumException("Unexpected element type " + mutation.getElementType());
            }

            return jsonBuilder.endObject();
        } catch (IOException ex) {
            throw new VertexiumException("Could not build document", ex);
        }
    }

    private String getElementTypeValueFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return ElasticsearchDocumentType.VERTEX.getKey();
            case EDGE:
                return ElasticsearchDocumentType.EDGE.getKey();
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            return mutationStore.getVertex(vertexId, fetchHints, endTime, user);
        }
        if (vertexId == null) {
            return null;
        }
        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .has(Element.ID_PROPERTY_NAME, vertexId)
            .limit(1L)
            .vertices(fetchHints))
            .findFirst()
            .orElse(null);
    }

    @Override
    public Stream<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            throw new VertexiumException("not implemented");
        }
        Query q = query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .has(Element.ID_PROPERTY_NAME, Compare.STARTS_WITH, vertexIdPrefix)
            .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING);
        return stream(q.vertices(fetchHints));
    }

    @Override
    public Stream<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            throw new VertexiumException("not implemented");
        }
        Query q = query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .has(Element.ID_PROPERTY_NAME, Compare.RANGE, idRange)
            .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING);
        return stream(q.vertices(fetchHints));

    }

    @Override
    public Stream<Vertex> getVertices(FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            throw new VertexiumException("not implemented");
        }
        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .vertices(fetchHints));
    }

    @Override
    public Stream<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            // TODO could probably do this in one query
            return stream(ids)
                .map(id -> getVertex(id, fetchHints, endTime, user))
                .filter(Objects::nonNull);
        }
        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .has(Element.ID_PROPERTY_NAME, Contains.IN, ids)
            .vertices(fetchHints));
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        addValidAuthorizations(visibility);
        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                User user = authorizations.getUser();
                saveMutation(this, timestamp, user);
                flush();
                return getEdge(getId(), user);
            }

            @Override
            public String save(User user) {
                saveMutation(this, timestamp, user);
                return getId();
            }
        };
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility) {
        addValidAuthorizations(visibility);
        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                User user = authorizations.getUser();
                saveMutation(this, timestamp, user);
                flush();
                return getEdge(getId(), user);
            }

            @Override
            public String save(User user) {
                saveMutation(this, timestamp, user);
                return getId();
            }
        };
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            return mutationStore.getEdges(edgeId, fetchHints, endTime, user);
        }
        // TODO switch to new query api
        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations()))
            .has(Element.ID_PROPERTY_NAME, edgeId)
            .limit(1L)
            .edges(fetchHints))
            .findFirst()
            .orElse(null);
    }

    @Override
    public Stream<Edge> getEdges(FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            throw new VertexiumException("not implemented");
        }
        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .edges(fetchHints));
    }

    @Override
    public Stream<Edge> getEdgesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            throw new VertexiumException("not implemented");
        }
        Query q = query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .has(Element.ID_PROPERTY_NAME, Compare.RANGE, idRange)
            .sort(Element.ID_PROPERTY_NAME, SortDirection.ASCENDING);
        return stream(q.edges(fetchHints));
    }

    @Override
    public Stream<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        if (endTime != null) {
            throw new VertexiumException("not implemented");
        }
        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .has(Element.ID_PROPERTY_NAME, Contains.IN, ids)
            .edges(fetchHints));
    }

    @Override
    public void shutdown() {
        client.close();
    }

    @Override
    public void flush() {
        super.flush();
        flushObjectQueue.flush();
    }

    @Override
    public void flushGraph() {
        flushObjectQueue.flush();
    }

    @Override
    public Stream<Path> findPaths(FindPathOptions options, User user) {
        validateAuthorizations(user);
        ProgressCallback progressCallback = options.getProgressCallback();
        if (progressCallback == null) {
            progressCallback = new ProgressCallback() {
                @Override
                public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                    LOGGER.debug("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount));
                }
            };
        }

        return new FindPathStrategy(this, options, progressCallback, user).findPaths();
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, User user) {
        return user.canRead(visibility);
    }

    @Override
    public void truncate() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void drop() {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        addValidAuthorizations(auths);
        return new Elasticsearch5GraphAuthorizations(auths);
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
            .map(StreamingPropertyValue::getInputStream)
            .collect(Collectors.toList());
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, FetchHints fetchHints, User user) {
        validateAuthorizations(user);
        List<HasExtendedDataFilter> filters = stream(ids)
            .map(id -> new HasExtendedDataFilter(id.getElementType(), id.getElementId(), id.getTableName(), id.getRowId()))
            .collect(Collectors.toList());
        Query q = query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .hasExtendedData(filters);
        return stream(q.extendedDataRows());
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        User user
    ) {
        validateAuthorizations(user);
        if ((elementType == null && (elementId != null || tableName != null))
            || (elementType != null && elementId == null && tableName != null)) {
            throw new VertexiumException("Cannot create partial key with missing inner value");
        }

        return stream(query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations()))
            .hasExtendedData(elementType, elementId, tableName)
            .extendedDataRows(fetchHints));
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, IdRange elementIdRange, User user) {
        validateAuthorizations(user);
        Query q = query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations())) // TODO switch to new query
            .has(ExtendedDataRow.ELEMENT_TYPE, elementType)
            .has(ExtendedDataRow.ELEMENT_ID, Compare.RANGE, elementIdRange)
            .sort(ExtendedDataRow.ELEMENT_ID, SortDirection.ASCENDING);
        return stream(q.extendedDataRows());
    }

    public Client getClient() {
        return client;
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return graphMetadataStore;
    }

    public QueryBuilder getAdditionalVisibilitiesFilter(User user) {
        validateAuthorizations(user);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (GraphMetadataEntry metadata : getMetadataWithPrefix(ADDITIONAL_VISIBILITY_METADATA_PREFIX)) {
            String visibilityString = (String) metadata.getValue();
            if (!user.canRead(new Visibility(visibilityString))) {
                boolQuery.mustNot(QueryBuilders.termQuery(FieldNames.ADDITIONAL_VISIBILITY, visibilityString));
            }
        }
        return boolQuery;
    }

    void ensureAdditionalVisibilitiesDefinedExtendedData(Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalVisibilities) {
        ensureAdditionalVisibilitiesDefined(new ConvertingIterable<AdditionalExtendedDataVisibilityAddMutation, Visibility>(additionalVisibilities) {
            @Override
            protected Visibility convert(AdditionalExtendedDataVisibilityAddMutation o) {
                return o.getAdditionalVisibility();
            }
        });
    }

    void ensureAdditionalVisibilitiesDefinedMutations(Iterable<AdditionalVisibilityAddMutation> additionalVisibilities) {
        ensureAdditionalVisibilitiesDefined(new ConvertingIterable<AdditionalVisibilityAddMutation, Visibility>(additionalVisibilities) {
            @Override
            protected Visibility convert(AdditionalVisibilityAddMutation o) {
                return o.getAdditionalVisibility();
            }
        });
    }

    void ensureAdditionalVisibilitiesDefined(Iterable<Visibility> additionalVisibilities) {
        for (Visibility additionalVisibility : additionalVisibilities) {
            if (!additionalVisibilitiesCache.contains(additionalVisibility)) {
                String key = ADDITIONAL_VISIBILITY_METADATA_PREFIX + additionalVisibility;
                if (getMetadata(key) == null) {
                    setMetadata(key, additionalVisibility);
                }
                additionalVisibilitiesCache.add(additionalVisibility);
            }
        }
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public IdStrategy getIdStrategy() {
        return idStrategy;
    }

    public PropertyNameService getPropertyNameService() {
        return propertyNameService;
    }

    public PropertyNameVisibilitiesStore getPropertyNameVisibilityStore() {
        return propertyNameVisibilityStore;
    }

    public StreamingPropertyValueService getStreamingPropertyValueService() {
        return streamingPropertyValueService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    @Override
    public Elasticsearch5GraphConfiguration getConfiguration() {
        return (Elasticsearch5GraphConfiguration) super.getConfiguration();
    }

    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchVertexQuery(
            getClient(),
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilityStore,
            idStrategy,
            queryString,
            vertex,
            new ElasticsearchSearchVertexQuery.Options()
                .setIndexSelectionStrategy(getIndexSelectionStrategy())
                .setPageSize(getConfiguration().getQueryPageSize())
                .setPagingLimit(getConfiguration().getPagingLimit())
                .setScrollKeepAlive(getConfiguration().getScrollKeepAlive())
                .setTermAggregationShardSize(getConfiguration().getTermAggregationShardSize())
                .setMaxQueryStringTerms(getConfiguration().getMaxQueryStringTerms()),
            authorizations
        );
    }

    public boolean isPropertyInIndex(String field) {
        return indexService.isPropertyInIndex(field);
    }

    public String[] getPropertyNames(String propertyName, User user) {
        validateAuthorizations(user);
        String[] allMatchingPropertyNames = propertyNameService.getAllMatchingPropertyNames(propertyName, user);
        return Arrays.stream(allMatchingPropertyNames)
            .map(propertyNameService::replaceFieldnameDots)
            .collect(Collectors.toList())
            .toArray(new String[allMatchingPropertyNames.length]);
    }

    public boolean supportsExactMatchSearch(PropertyDefinition propertyDefinition) {
        return propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH) || propertyDefinition.isSortable();
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticsearchSearchGraphQuery(
            getClient(),
            this,
            indexService,
            propertyNameService,
            propertyNameVisibilityStore,
            idStrategy,
            similarToFields,
            similarToText,
            new ElasticsearchSearchQueryBase.Options()
                .setIndexSelectionStrategy(getIndexSelectionStrategy())
                .setPageSize(getConfiguration().getQueryPageSize())
                .setPagingLimit(getConfiguration().getPagingLimit())
                .setScrollKeepAlive(getConfiguration().getScrollKeepAlive())
                .setTermAggregationShardSize(getConfiguration().getTermAggregationShardSize())
                .setMaxQueryStringTerms(getConfiguration().getMaxQueryStringTerms()),
            authorizations
        );
    }

    public void handleDocumentMissingException(FlushObjectQueue.FlushObject flushObject, Exception ex) {
        if (exceptionHandler == null) {
            LOGGER.error("document missing: " + flushObject, ex);
            return;
        }
        exceptionHandler.handleDocumentMissingException(this, flushObject, ex);
    }

    MutationStore getMutationStore() {
        return mutationStore;
    }
}
