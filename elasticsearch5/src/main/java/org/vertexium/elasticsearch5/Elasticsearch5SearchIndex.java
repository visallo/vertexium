package org.vertexium.elasticsearch5;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.internal.ConcurrentSet;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.vertexium.*;
import org.vertexium.elasticsearch5.utils.DefaultBulkProcessorListener;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;
import org.vertexium.mutation.*;
import org.vertexium.query.*;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.ExtendedDataMutationUtils;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.vertexium.elasticsearch5.utils.SearchResponseUtils.checkForFailures;

public class Elasticsearch5SearchIndex implements SearchIndex {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(Elasticsearch5SearchIndex.class);
    protected static final VertexiumLogger MUTATION_LOGGER = VertexiumLoggerFactory.getMutationLogger(SearchIndex.class);
    public static final String ELEMENT_ID_FIELD_NAME = "__elementId";
    public static final String ELEMENT_TYPE_FIELD_NAME = "__elementType";
    public static final String VISIBILITY_FIELD_NAME = "__visibility";
    public static final String HIDDEN_VERTEX_FIELD_NAME = "__hidden";
    public static final String HIDDEN_PROPERTY_FIELD_NAME = "__hidden_property";
    public static final String OUT_VERTEX_ID_FIELD_NAME = "__outVertexId";
    public static final String IN_VERTEX_ID_FIELD_NAME = "__inVertexId";
    public static final String EDGE_LABEL_FIELD_NAME = "__edgeLabel";
    public static final String EXTENDED_DATA_TABLE_NAME_FIELD_NAME = "__extendedDataTableName";
    public static final String EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME = "__extendedDataRowId";
    public static final String EXTENDED_DATA_TABLE_COLUMN_VISIBILITIES_FIELD_NAME = "__extendedDataColumnVisibilities";
    public static final String ADDITIONAL_VISIBILITY_FIELD_NAME = "__additionalVisibility";
    public static final String ADDITIONAL_VISIBILITY_METADATA_PREFIX = "elasticsearch_additionalVisibility_";
    public static final String EXACT_MATCH_FIELD_NAME = "exact";
    public static final String EXACT_MATCH_PROPERTY_NAME_SUFFIX = "." + EXACT_MATCH_FIELD_NAME;
    public static final String GEO_PROPERTY_NAME_SUFFIX = "_g";
    public static final String GEO_POINT_PROPERTY_NAME_SUFFIX = "_gp"; // Used for geo hash aggregation of geo points
    public static final String LOWERCASER_NORMALIZER_NAME = "visallo_lowercaser";
    public static final int EXACT_MATCH_IGNORE_ABOVE_LIMIT = 10000;
    public static final String FIELDNAME_DOT_REPLACEMENT = "-_-";
    private final Set<String> additionalVisibilitiesCache = new ConcurrentSet<>();
    private final Client client;
    private final ElasticsearchSearchIndexConfiguration config;
    private final Graph graph;
    private final IndexService indexService;
    private final PropertyNameService propertyNameService;
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;
    private IndexSelectionStrategy indexSelectionStrategy;
    public static final Pattern AGGREGATION_NAME_PATTERN = Pattern.compile("(.*?)_([0-9a-f]+)");
    private final FlushObjectQueue flushObjectQueue;
    private boolean serverPluginInstalled;
    private final IdStrategy idStrategy = new IdStrategy();
    private final Elasticsearch5ExceptionHandler exceptionHandler;
    private final AddElementService addElementService;
    private final AddOrUpdateService addOrUpdateService;
    private final ExtendedDataService extendedDataService;
    private final Integer logRequestSizeLimit;

    public Elasticsearch5SearchIndex(Graph graph, GraphConfiguration config) {
        this.graph = graph;
        this.config = new ElasticsearchSearchIndexConfiguration(graph, config);
        this.indexSelectionStrategy = this.config.getIndexSelectionStrategy();
        this.propertyNameVisibilitiesStore = this.config.createPropertyNameVisibilitiesStore(graph);
        this.client = createClient(this.config);
        this.serverPluginInstalled = checkPluginInstalled(this.client);
        this.exceptionHandler = this.config.getExceptionHandler(graph);
        this.flushObjectQueue = new FlushObjectQueue(this);
        this.propertyNameService = new PropertyNameService(propertyNameVisibilitiesStore);
        this.logRequestSizeLimit = this.config.getLogRequestSizeLimit();
        this.indexService = new IndexService(
            graph,
            this,
            client,
            this.config,
            indexSelectionStrategy,
            idStrategy,
            propertyNameService,
            propertyNameVisibilitiesStore
        );
        this.addElementService = new AddElementService(
            graph,
            this,
            flushObjectQueue,
            idStrategy,
            indexService,
            propertyNameService
        );
        this.addOrUpdateService = new AddOrUpdateService(
            graph,
            this,
            client,
            this.config,
            indexService,
            idStrategy,
            propertyNameService,
            flushObjectQueue
        );
        this.extendedDataService = new ExtendedDataService(
            this,
            client,
            this.config,
            idStrategy,
            propertyNameService,
            indexService
        );

        storePainlessScript("deleteFieldsFromDocumentScript", "remove-fields-from-document.painless");
        storePainlessScript("updateFieldsOnDocumentScript", "update-fields-on-document.painless");
    }

    private void storePainlessScript(String scriptId, String scriptSourceName) {
        try (
            InputStream scriptSource = getClass().getResourceAsStream(scriptSourceName);
            InputStream helperSource = getClass().getResourceAsStream("helper-functions.painless")
        ) {
            String source = IOUtils.toString(helperSource) + " " + IOUtils.toString(scriptSource);
            source = source.replaceAll("\\r?\\n", " ").replaceAll("\"", "\\\\\"");
            client.admin().cluster().preparePutStoredScript()
                .setId(scriptId)
                .setContent(new BytesArray("{\"script\": {\"lang\": \"painless\", \"source\": \"" + source + "\"}}"), XContentType.JSON)
                .get();
        } catch (Exception ex) {
            throw new VertexiumException("Could not load painless script: " + scriptId, ex);
        }
    }

    protected Client createClient(ElasticsearchSearchIndexConfiguration config) {
        return createTransportClient(config);
    }

    private static TransportClient createTransportClient(ElasticsearchSearchIndexConfiguration config) {
        Settings settings = tryReadSettingsFromFile(config);
        if (settings == null) {
            Settings.Builder settingsBuilder = Settings.builder();
            if (config.getClusterName() != null) {
                settingsBuilder.put("cluster.name", config.getClusterName());
            }
            for (Map.Entry<String, String> esSetting : config.getEsSettings().entrySet()) {
                settingsBuilder.put(esSetting.getKey(), esSetting.getValue());
            }
            settings = settingsBuilder.build();
        }
        Collection<Class<? extends Plugin>> plugins = loadTransportClientPlugins(config);
        TransportClient transportClient = new PreBuiltTransportClient(settings, plugins);
        for (String esLocation : config.getEsLocations()) {
            String[] locationSocket = esLocation.split(":");
            String hostname;
            int port;
            if (locationSocket.length == 2) {
                hostname = locationSocket[0];
                port = Integer.parseInt(locationSocket[1]);
            } else if (locationSocket.length == 1) {
                hostname = locationSocket[0];
                port = config.getPort();
            } else {
                throw new VertexiumException("Invalid elastic search location: " + esLocation);
            }
            InetAddress host;
            try {
                host = InetAddress.getByName(hostname);
            } catch (UnknownHostException ex) {
                throw new VertexiumException("Could not resolve host: " + hostname, ex);
            }
            transportClient.addTransportAddress(new InetSocketTransportAddress(host, port));
        }
        return transportClient;
    }

    @SuppressWarnings("unchecked")
    private static Collection<Class<? extends Plugin>> loadTransportClientPlugins(ElasticsearchSearchIndexConfiguration config) {
        return config.getEsPluginClassNames().stream()
            .map(pluginClassName -> {
                try {
                    return (Class<? extends Plugin>) Class.forName(pluginClassName);
                } catch (ClassNotFoundException ex) {
                    throw new VertexiumException("Could not load transport client plugin: " + pluginClassName, ex);
                }
            })
            .collect(Collectors.toList());
    }

    private static Settings tryReadSettingsFromFile(ElasticsearchSearchIndexConfiguration config) {
        File esConfigFile = config.getEsConfigFile();
        if (esConfigFile == null) {
            return null;
        }
        if (!esConfigFile.exists()) {
            throw new VertexiumException(esConfigFile.getAbsolutePath() + " does not exist");
        }
        try (FileInputStream fileIn = new FileInputStream(esConfigFile)) {
            return Settings.builder().loadFromStream(esConfigFile.getAbsolutePath(), fileIn).build();
        } catch (IOException e) {
            throw new VertexiumException("Could not read ES config file: " + esConfigFile.getAbsolutePath(), e);
        }
    }

    private boolean checkPluginInstalled(Client client) {
        if (config.isForceDisableVertexiumPlugin()) {
            LOGGER.info("Forcing the vertexium plugin off. Running without the server side Vertexium plugin will disable some features.");
            return false;
        }

        NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo().setPlugins(true).get();
        for (NodeInfo nodeInfo : nodesInfoResponse.getNodes()) {
            for (PluginInfo pluginInfo : nodeInfo.getPlugins().getPluginInfos()) {
                if ("vertexium".equals(pluginInfo.getName())) {
                    return true;
                }
            }
        }
        if (config.isErrorOnMissingVertexiumPlugin()) {
            throw new VertexiumException("Vertexium plugin cannot be found");
        }
        LOGGER.warn("Running without the server side Vertexium plugin will disable some features.");
        return false;
    }

    @Override
    public void clearCache() {
        indexService.clearCache();
    }

    @Override
    public void addElement(
        Graph graph,
        Element element,
        Set<String> additionalVisibilities,
        Set<String> additionalVisibilitiesToDelete,
        User user
    ) {
        addElementService.addElement(
            element,
            additionalVisibilities,
            additionalVisibilitiesToDelete
        );
    }

    @Override
    public <TElement extends Element> void addOrUpdateElement(Graph graph, ElementMutation<TElement> mutation, User user) {
        if (mutation.isDeleteElement() || mutation.getSoftDeleteData() != null) {
            deleteElement(graph, mutation, user);
        } else {
            addOrUpdateService.addOrUpdateElement(mutation);
        }
    }

    @Override
    public void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        User user
    ) {
        Map<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowId = ExtendedDataMutationUtils.getByTableThenRowId(
            extendedData,
            null,
            null,
            additionalExtendedDataVisibilities,
            additionalExtendedDataVisibilityDeletes
        );

        for (Map.Entry<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowIdEntry : byTableThenRowId.entrySet()) {
            String tableName = byTableThenRowIdEntry.getKey();
            Map<String, ExtendedDataMutationUtils.Mutations> byRow = byTableThenRowIdEntry.getValue();
            for (Map.Entry<String, ExtendedDataMutationUtils.Mutations> byRowEntry : byRow.entrySet()) {
                String rowId = byRowEntry.getKey();
                ExtendedDataMutationUtils.Mutations mutations = byRowEntry.getValue();
                extendedDataService.addElementExtendedData(
                    graph,
                    elementLocation,
                    tableName,
                    rowId,
                    mutations.getExtendedData(),
                    mutations.getAdditionalExtendedDataVisibilities(),
                    mutations.getAdditionalExtendedDataVisibilityDeletes()
                );
            }
        }
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId rowId, User user) {
        String indexName = indexService.getExtendedDataIndexName(rowId);
        String docId = getIdStrategy().createExtendedDataDocId(rowId);
        indexService.pushChange(indexName);
        getClient().prepareDelete(indexName, getIdStrategy().getType(), docId).execute().actionGet();
    }

    @Override
    public void deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        User user
    ) {
        String extendedDataDocId = getIdStrategy().createExtendedDataDocId(elementLocation, tableName, row);
        String fieldName = propertyNameService.addVisibilityToPropertyName(graph, columnName, visibility);
        String indexName = indexService.getExtendedDataIndexName(elementLocation, tableName, row);
        removeFieldsFromDocument(
            graph,
            indexName,
            elementLocation,
            extendedDataDocId,
            Lists.newArrayList(fieldName, fieldName + "_e")
        );
    }

    @Override
    public void addExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataRow> extendedDatas,
        User user
    ) {
        extendedDataService.addExtendedData(graph, elementLocation, extendedDatas);
    }

    @Override
    public <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        User user
    ) {
        // Remove old element field name
        String oldFieldName = propertyNameService.addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, oldVisibility);
        removeFieldsFromDocument(graph, elementMutation, oldFieldName);

        addOrUpdateElement(graph, elementMutation, user);
    }

    @Override
    public void addAdditionalVisibility(
        Graph graph,
        Element element,
        String visibility,
        Object eventData,
        User user
    ) {
        String indexName = indexService.getIndexName(element);
        String documentId = getIdStrategy().createElementDocId(element);
        UpdateRequestBuilder updateRequestBuilder = prepareUpdateFieldsOnDocument(
            indexName,
            documentId,
            element,
            null,
            null,
            null,
            Sets.newHashSet(visibility),
            null
        );
        if (updateRequestBuilder != null) {
            indexService.pushChange(indexName);
            addActionRequestBuilderForFlush(element, updateRequestBuilder);

            if (getConfig().isAutoFlush()) {
                flush(graph);
            }
        }
    }

    @Override
    public void deleteAdditionalVisibility(
        Graph graph,
        Element element,
        String visibility,
        Object eventData,
        User user
    ) {
        String indexName = indexService.getIndexName(element);
        String documentId = getIdStrategy().createElementDocId(element);
        UpdateRequestBuilder updateRequestBuilder = prepareUpdateFieldsOnDocument(
            indexName,
            documentId,
            element,
            null,
            null,
            null,
            null,
            Sets.newHashSet(visibility)
        );
        if (updateRequestBuilder != null) {
            indexService.pushChange(indexName);
            addActionRequestBuilderForFlush(element, updateRequestBuilder);

            if (getConfig().isAutoFlush()) {
                flush(graph);
            }
        }
    }

    public boolean isServerPluginInstalled() {
        return serverPluginInstalled;
    }

    public void handleDocumentMissingException(FlushObjectQueue.FlushObject flushObject, Exception ex) throws Exception {
        if (exceptionHandler == null) {
            LOGGER.error("document missing: " + flushObject, ex);
            return;
        }
        exceptionHandler.handleDocumentMissingException(graph, this, flushObject, ex);
    }

    public boolean supportsExactMatchSearch(PropertyDefinition propertyDefinition) {
        return propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH) || propertyDefinition.isSortable();
    }

    public String getAggregationName(String name) {
        Matcher m = AGGREGATION_NAME_PATTERN.matcher(name);
        if (m.matches()) {
            return m.group(1);
        }
        throw new VertexiumException("Could not get aggregation name from: " + name);
    }

    private void deleteElement(Graph graph, ElementLocation elementLocation, User user) {
        deleteExtendedDataForElement(elementLocation);

        String indexName = indexService.getIndexName(elementLocation);
        String docId = getIdStrategy().createElementDocId(elementLocation);
        if (MUTATION_LOGGER.isTraceEnabled()) {
            LOGGER.trace("deleting document %s (docId: %s)", elementLocation.getId(), docId);
        }
        indexService.pushChange(indexName);
        getClient().delete(
            getClient()
                .prepareDelete(indexName, getIdStrategy().getType(), docId)
                .request()
        ).actionGet();
    }

    private void deleteExtendedDataForElement(ElementLocation elementLocatiaon) {
        try {
            QueryBuilder filter = QueryBuilders.termQuery(ELEMENT_ID_FIELD_NAME, elementLocatiaon.getId());

            SearchRequestBuilder s = getClient().prepareSearch(indexService.getIndicesToQuery())
                .setTypes(getIdStrategy().getType())
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(filter))
                .storedFields(
                    ELEMENT_ID_FIELD_NAME,
                    EXTENDED_DATA_TABLE_NAME_FIELD_NAME,
                    EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME
                );
            SearchResponse searchResponse = checkForFailures(s.execute().get());
            for (SearchHit hit : searchResponse.getHits()) {
                if (MUTATION_LOGGER.isTraceEnabled()) {
                    LOGGER.trace("deleting extended data document %s", hit.getId());
                }
                indexService.pushChange(hit.getIndex());
                getClient().prepareDelete(hit.getIndex(), hit.getType(), hit.getId()).execute().actionGet();
            }
        } catch (Exception ex) {
            throw new VertexiumException("Could not delete extended data for element: " + elementLocatiaon.getId());
        }
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.PROPERTY;
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchGraphQuery(
            getClient(),
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            queryString,
            new ElasticsearchSearchQueryBase.Options()
                .setIndexSelectionStrategy(getIndexSelectionStrategy())
                .setPageSize(getConfig().getQueryPageSize())
                .setPagingLimit(getConfig().getPagingLimit())
                .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchVertexQuery(
            getClient(),
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            queryString,
            vertex,
            new ElasticsearchSearchVertexQuery.Options()
                .setIndexSelectionStrategy(getIndexSelectionStrategy())
                .setPageSize(getConfig().getQueryPageSize())
                .setPagingLimit(getConfig().getPagingLimit())
                .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchExtendedDataQuery(
            getClient(),
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            queryString,
            element.getId(),
            tableName,
            new ElasticsearchSearchExtendedDataQuery.Options()
                .setIndexSelectionStrategy(getIndexSelectionStrategy())
                .setPageSize(getConfig().getQueryPageSize())
                .setPagingLimit(getConfig().getPagingLimit())
                .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticsearchSearchGraphQuery(
            getClient(),
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            similarToFields,
            similarToText,
            new ElasticsearchSearchQueryBase.Options()
                .setIndexSelectionStrategy(getIndexSelectionStrategy())
                .setPageSize(getConfig().getQueryPageSize())
                .setPagingLimit(getConfig().getPagingLimit())
                .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, User user) {
        deleteProperties(graph, element, Collections.singletonList(property), user);
    }

    @Override
    public void deleteProperties(Graph graph, Element element, Collection<PropertyDescriptor> propertyList, User user) {
        List<String> fieldsToRemove = new ArrayList<>();
        Map<String, Object> fieldsToSet = new HashMap<>();
        Set<PropertyDescriptor> propertyValuesToRemove = new HashSet<>();
        propertyList.forEach(p -> {
            String fieldName = propertyNameService.addVisibilityToPropertyName(graph, p.getName(), p.getVisibility());
            fieldsToRemove.add(fieldName);
            propertyValuesToRemove.add(p);
            indexService.addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet, propertyValuesToRemove);
        });

        String documentId = getIdStrategy().createElementDocId(element);
        String indexName = indexService.getIndexName(element);
        IndexInfo indexInfo = indexService.ensureIndexCreatedAndInitialized(indexName);
        UpdateRequestBuilder updateRequestBuilder = prepareUpdateFieldsOnDocument(
            indexInfo.getIndexName(),
            documentId,
            element,
            fieldsToSet,
            fieldsToRemove,
            null,
            null,
            null
        );
        if (updateRequestBuilder != null) {
            indexService.pushChange(indexInfo.getIndexName());
            addActionRequestBuilderForFlush(element, updateRequestBuilder);

            if (getConfig().isAutoFlush()) {
                flush(graph);
            }
        }
    }

    void bulkUpdate(Graph graph, Iterable<UpdateRequest> updateRequests) {
        int totalCount = 0;
        List<Throwable> failures = new ArrayList<>();
        BulkProcessor.Builder builder = BulkProcessor.builder(
            getClient(),
            new DefaultBulkProcessorListener() {
                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    LOGGER.error("Failed bulk request: %s", request.toString(), failure);
                    failures.add(failure);
                }
            }
        );
        BulkProcessor bulkProcessor = builder.build();
        for (UpdateRequest updateRequest : updateRequests) {
            bulkProcessor.add(updateRequest);
            totalCount++;
        }
        bulkProcessor.flush();
        try {
            // We should never wait this long, but setting it high just to be sure everything is finished
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            throw new VertexiumException("Failed bulk update, waiting for close", ex);
        }
        LOGGER.debug("added %d elements", totalCount);
        if (failures.size() > 0) {
            throw new VertexiumException(String.format("Failed bulk update (failures: %d)", failures.size()));
        }

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    @Override
    public MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchMultiVertexQuery(
            getClient(),
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            queryString,
            vertexIds,
            new ElasticsearchSearchQueryBase.Options()
                .setIndexSelectionStrategy(getIndexSelectionStrategy())
                .setPageSize(getConfig().getQueryPageSize())
                .setPagingLimit(getConfig().getPagingLimit())
                .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
            authorizations
        );
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return true;
    }

    @Override
    public void flush(Graph graph) {
        flushObjectQueue.flush();
    }

    private void removeFieldsFromDocument(Graph graph, ElementLocation elementLocation, String field) {
        removeFieldsFromDocument(graph, elementLocation, Lists.newArrayList(field));
    }

    private void removeFieldsFromDocument(Graph graph, ElementLocation elementLocation, Collection<String> fields) {
        String indexName = indexService.getIndexName(elementLocation);
        String documentId = getIdStrategy().createElementDocId(elementLocation);
        removeFieldsFromDocument(graph, indexName, elementLocation, documentId, fields);
    }

    private void removeFieldsFromDocument(
        Graph graph,
        String indexName,
        ElementLocation elementLocation,
        String documentId,
        Collection<String> fields
    ) {
        if (fields == null || fields.isEmpty()) {
            return;
        }

        indexService.pushChange(indexName);
        UpdateRequestBuilder updateRequestBuilder = prepareRemoveFieldsFromDocument(indexName, documentId, fields);
        addActionRequestBuilderForFlush(elementLocation, updateRequestBuilder);

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    private UpdateRequestBuilder prepareRemoveFieldsFromDocument(String indexName, String documentId, Collection<String> fields) {
        List<String> fieldNames = fields.stream().map(propertyNameService::replaceFieldnameDots).collect(Collectors.toList());
        if (fieldNames.isEmpty()) {
            return null;
        }

        return getClient().prepareUpdate()
            .setIndex(indexName)
            .setId(documentId)
            .setType(getIdStrategy().getType())
            .setScript(new Script(
                ScriptType.STORED,
                "painless",
                "deleteFieldsFromDocumentScript",
                ImmutableMap.of("fieldNames", fieldNames)
            ))
            .setRetryOnConflict(FlushObjectQueue.MAX_RETRIES);
    }

    UpdateRequestBuilder prepareUpdateFieldsOnDocument(
        String indexName,
        String documentId,
        ElementLocation elementLocation,
        Map<String, Object> fieldsToSet,
        Collection<String> fieldsToRemove,
        Map<String, String> fieldsToRename,
        Set<String> additionalVisibilities,
        Set<String> additionalVisibilitiesToDelete
    ) {
        fieldsToSet = fieldsToSet == null ? Collections.emptyMap() : fieldsToSet.entrySet().stream()
            .collect(Collectors.toMap(e -> propertyNameService.replaceFieldnameDots(e.getKey()), Map.Entry::getValue));
        fieldsToRemove = fieldsToRemove == null ? Collections.emptyList() : fieldsToRemove.stream().map(propertyNameService::replaceFieldnameDots).collect(Collectors.toList());
        fieldsToRename = fieldsToRename == null ? Collections.emptyMap() : fieldsToRename.entrySet().stream()
            .collect(Collectors.toMap(e -> propertyNameService.replaceFieldnameDots(e.getKey()), e -> propertyNameService.replaceFieldnameDots(e.getValue())));
        List<String> additionalVisibilitiesParam = additionalVisibilities == null ? Collections.emptyList() : new ArrayList<>(additionalVisibilities);
        List<String> additionalVisibilitiesToDeleteParam = additionalVisibilitiesToDelete == null ? Collections.emptyList() : new ArrayList<>(additionalVisibilitiesToDelete);
        ensureAdditionalVisibilitiesDefined(additionalVisibilitiesParam);

        XContentBuilder jsonBuilder;
        try {
            jsonBuilder = addElementService.buildJsonContentFromElementLocation(elementLocation).endObject();
        } catch (IOException ex) {
            throw new VertexiumException("Could not create json builder", ex);
        }

        return getClient().prepareUpdate()
            .setIndex(indexName)
            .setId(documentId)
            .setUpsert(jsonBuilder)
            .setScriptedUpsert(true)
            .setType(getIdStrategy().getType())
            .setScript(new Script(
                ScriptType.STORED,
                "painless",
                "updateFieldsOnDocumentScript",
                ImmutableMap.of(
                    "fieldsToSet", fieldsToSet,
                    "fieldsToRemove", fieldsToRemove,
                    "fieldsToRename", fieldsToRename,
                    "additionalVisibilities", additionalVisibilitiesParam,
                    "additionalVisibilitiesToDelete", additionalVisibilitiesToDeleteParam
                )
            ))
            .setRetryOnConflict(FlushObjectQueue.MAX_RETRIES);
    }

    @Override
    public void shutdown() {
        client.close();

        if (propertyNameVisibilitiesStore instanceof Closeable) {
            try {
                ((Closeable) propertyNameVisibilitiesStore).close();
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    @Override
    public synchronized void truncate(Graph graph) {
        LOGGER.warn("Truncate of Elasticsearch is not possible, dropping the indices and recreating instead.");
        drop(graph);
    }

    @Override
    public void drop(Graph graph) {
        indexService.drop();
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public Client getClient() {
        return client;
    }

    public ElasticsearchSearchIndexConfiguration getConfig() {
        return config;
    }

    public IdStrategy getIdStrategy() {
        return idStrategy;
    }

    public QueryBuilder getAdditionalVisibilitiesFilter(User user) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(ADDITIONAL_VISIBILITY_METADATA_PREFIX)) {
            String visibilityString = (String) metadata.getValue();
            if (!user.canRead(new Visibility(visibilityString))) {
                boolQuery.mustNot(QueryBuilders.termQuery(ADDITIONAL_VISIBILITY_FIELD_NAME, visibilityString));
            }
        }
        return boolQuery;
    }

    public String[] getPropertyNames(Graph graph, String propertyName, User user) {
        String[] allMatchingPropertyNames = propertyNameService.getAllMatchingPropertyNames(graph, propertyName, user);
        return Arrays.stream(allMatchingPropertyNames)
            .map(propertyNameService::replaceFieldnameDots)
            .collect(Collectors.toList())
            .toArray(new String[allMatchingPropertyNames.length]);
    }

    @Override
    public org.vertexium.search.GraphQuery queryGraph(Graph graph, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public org.vertexium.search.MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public org.vertexium.search.VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public org.vertexium.search.Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, User user) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, User user) {
        throw new VertexiumException("not implemented");
    }

    void addActionRequestBuilderForFlush(
        ElementLocation elementLocation,
        UpdateRequestBuilder updateRequestBuilder
    ) {
        addActionRequestBuilderForFlush(
            elementLocation,
            null,
            null,
            updateRequestBuilder
        );
    }

    void addActionRequestBuilderForFlush(
        ElementLocation elementLocation,
        String extendedDataTableName,
        String rowId,
        UpdateRequestBuilder updateRequestBuilder
    ) {
        Future future;
        try {
            logRequestSize(elementLocation.getId(), updateRequestBuilder.request());
            future = updateRequestBuilder.execute();
        } catch (Exception ex) {
            LOGGER.debug("Could not execute update: %s", ex.getMessage());
            future = SettableFuture.create();
            ((SettableFuture) future).setException(ex);
        }
        flushObjectQueue.add(elementLocation, extendedDataTableName, rowId, updateRequestBuilder, future);
    }

    private void logRequestSize(String elementId, UpdateRequest request) {
        if (logRequestSizeLimit == null) {
            return;
        }
        int sizeInBytes = 0;
        if (request.doc() != null) {
            sizeInBytes += request.doc().source().length();
        }
        if (request.upsertRequest() != null) {
            sizeInBytes += request.upsertRequest().source().length();
        }
        if (request.script() != null) {
            sizeInBytes += request.script().getIdOrCode().length() * 2;
        }
        if (sizeInBytes > logRequestSizeLimit) {
            LOGGER.warn("Large document detected (id: %s). Size in bytes: %d", elementId, sizeInBytes);
        }
    }

    void ensureAdditionalVisibilitiesDefined(Iterable<String> additionalVisibilities) {
        for (String additionalVisibility : additionalVisibilities) {
            if (!additionalVisibilitiesCache.contains(additionalVisibility)) {
                String key = ADDITIONAL_VISIBILITY_METADATA_PREFIX + additionalVisibility;
                if (graph.getMetadata(key) == null) {
                    graph.setMetadata(key, additionalVisibility);
                }
                additionalVisibilitiesCache.add(additionalVisibility);
            }
        }
    }

    public boolean isPropertyInIndex(Graph graph, String field) {
        return indexService.isPropertyInIndex(graph, field);
    }

    public IndexService getIndexService() {
        return indexService;
    }
}
