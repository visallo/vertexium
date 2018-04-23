package org.vertexium.elasticsearch5;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.vertexium.*;
import org.vertexium.Edge;
import org.vertexium.elasticsearch5.utils.DefaultBulkProcessorListener;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.*;
import org.vertexium.search.SearchIndex;
import org.vertexium.search.SearchIndexWithVertexPropertyCountByValue;
import org.vertexium.type.*;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.common.geo.builders.ShapeBuilder.*;
import static org.vertexium.elasticsearch5.ElasticsearchPropertyNameInfo.PROPERTY_NAME_PATTERN;
import static org.vertexium.elasticsearch5.utils.SearchResponseUtils.checkForFailures;
import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

public class Elasticsearch5SearchIndex implements SearchIndex, SearchIndexWithVertexPropertyCountByValue {
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
    public static final String EXACT_MATCH_FIELD_NAME = "exact";
    public static final String EXACT_MATCH_PROPERTY_NAME_SUFFIX = "." + EXACT_MATCH_FIELD_NAME;
    public static final String GEO_PROPERTY_NAME_SUFFIX = "_g";
    public static final String GEO_POINT_PROPERTY_NAME_SUFFIX = "_gp"; // Used for geo hash aggregation of geo points
    public static final String LOWERCASER_NORMALIZER_NAME = "visallo_lowercaser";
    public static final int EXACT_MATCH_IGNORE_ABOVE_LIMIT = 10000;
    public static final String FIELDNAME_DOT_REPLACEMENT = "-_-";
    private static final long IN_PROCESS_NODE_WAIT_TIME_MS = 10 * 60 * 1000;
    private static final int MAX_RETRIES = 10;
    private final Client client;
    private final ElasticsearchSearchIndexConfiguration config;
    private Map<String, IndexInfo> indexInfos;
    private final ReadWriteLock indexInfosLock = new ReentrantReadWriteLock();
    private int indexInfosLastSize = -1; // Used to prevent creating a index name array each time
    private String[] indexNamesAsArray;
    private IndexSelectionStrategy indexSelectionStrategy;
    private boolean allFieldEnabled;
    private Node inProcessNode;
    public static final Pattern AGGREGATION_NAME_PATTERN = Pattern.compile("(.*?)_([0-9a-f]+)");
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;
    private final FlushObjectQueue flushObjectQueue = new FlushObjectQueue();
    private final String geoShapePrecision;
    private final String geoShapeErrorPct;
    private boolean serverPluginInstalled;
    private final IdStrategy idStrategy = new IdStrategy();
    private final IndexRefreshTracker indexRefreshTracker = new IndexRefreshTracker();

    public Elasticsearch5SearchIndex(Graph graph, GraphConfiguration config) {
        this.config = new ElasticsearchSearchIndexConfiguration(graph, config);
        this.indexSelectionStrategy = this.config.getIndexSelectionStrategy();
        this.allFieldEnabled = this.config.isAllFieldEnabled(false);
        this.propertyNameVisibilitiesStore = this.config.createPropertyNameVisibilitiesStore(graph);
        this.client = createClient(this.config);
        this.serverPluginInstalled = checkPluginInstalled(this.client);
        this.geoShapePrecision = this.config.getGeoShapePrecision();
        this.geoShapeErrorPct = this.config.getGeoShapeErrorPct();
    }

    public PropertyNameVisibilitiesStore getPropertyNameVisibilitiesStore() {
        return propertyNameVisibilitiesStore;
    }

    protected Client createClient(ElasticsearchSearchIndexConfiguration config) {
        if (config.isInProcessNode()) {
            return createInProcessNode(config);
        } else {
            return createTransportClient(config);
        }
    }

    private Client createInProcessNode(ElasticsearchSearchIndexConfiguration config) {
        Settings settings = tryReadSettingsFromFile(config);
        if (settings == null) {
            String homePath = config.getInProcessNodeHomePath();
            checkNotNull(homePath, ElasticsearchSearchIndexConfiguration.IN_PROCESS_NODE_HOME_PATH + " is required for in process Elasticsearch node");

            Map<String, String> mapSettings = new HashMap<>();
            mapSettings.put("transport.type", "local");
            mapSettings.put("path.home", homePath);
            mapSettings.put("http.enabled", "false");
            mapSettings.put("discovery.zen.ping.unicast.hosts", "localhost");
            if (config.getClusterName() != null) {
                mapSettings.put("cluster.name", config.getClusterName());
            }

            mapSettings.putAll(config.getInProcessNodeAdditionalSettings());

            settings = Settings.builder()
                    .put(mapSettings)
                    .build();
        }

        this.inProcessNode = new Node(settings);
        try {
            inProcessNode.start();
        } catch (NodeValidationException ex) {
            throw new VertexiumException("Could not start in process node", ex);
        }
        Client client = inProcessNode.client();

        long startTime = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() > startTime + IN_PROCESS_NODE_WAIT_TIME_MS) {
                throw new VertexiumException("Status failed to exit red status after waiting " + IN_PROCESS_NODE_WAIT_TIME_MS + "ms. Giving up.");
            }
            ClusterHealthResponse health = client.admin().cluster().prepareHealth().get();
            if (health.getStatus() != ClusterHealthStatus.RED) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new VertexiumException("Could not sleep", e);
            }
            LOGGER.info("Status is %s, waiting...", health.getStatus());
        }
        return client;
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
        TransportClient transportClient = new PreBuiltTransportClient(settings);
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

    protected final boolean isAllFieldEnabled() {
        return allFieldEnabled;
    }

    public Set<String> getIndexNamesFromElasticsearch() {
        return client.admin().indices().prepareStats().execute().actionGet().getIndices().keySet();
    }

    @Override
    public void clearCache() {
        indexInfosLock.writeLock().lock();
        try {
            this.indexInfos = null;
            this.indexInfosLastSize = -1;
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    private Map<String, IndexInfo> getIndexInfos(Graph graph) {
        indexInfosLock.readLock().lock();
        try {
            if (this.indexInfos != null) {
                return new HashMap<>(this.indexInfos);
            }
        } finally {
            indexInfosLock.readLock().unlock();
        }
        return loadIndexInfos(graph);
    }

    private Map<String, IndexInfo> loadIndexInfos(Graph graph) {
        indexInfosLock.writeLock().lock();
        try {
            this.indexInfos = new HashMap<>();

            Set<String> indices = getIndexNamesFromElasticsearch();
            for (String indexName : indices) {
                if (!indexSelectionStrategy.isIncluded(this, indexName)) {
                    LOGGER.debug("skipping index %s, not in indicesToQuery", indexName);
                    continue;
                }

                LOGGER.debug("loading index info for %s", indexName);
                IndexInfo indexInfo = createIndexInfo(indexName);
                loadExistingMappingIntoIndexInfo(graph, indexInfo, indexName);
                indexInfo.setElementTypeDefined(indexInfo.isPropertyDefined(ELEMENT_TYPE_FIELD_NAME));
                addPropertyNameVisibility(graph, indexInfo, ELEMENT_ID_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, ELEMENT_TYPE_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, VISIBILITY_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, OUT_VERTEX_ID_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, IN_VERTEX_ID_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, EDGE_LABEL_FIELD_NAME, null);
                indexInfos.put(indexName, indexInfo);
            }
            return new HashMap<>(this.indexInfos);
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    private void loadExistingMappingIntoIndexInfo(Graph graph, IndexInfo indexInfo, String indexName) {
        try {
            GetMappingsResponse mapping = client.admin().indices().prepareGetMappings(indexName).get();
            for (ObjectCursor<String> mappingIndexName : mapping.getMappings().keys()) {
                ImmutableOpenMap<String, MappingMetaData> typeMappings = mapping.getMappings().get(mappingIndexName.value);
                for (ObjectCursor<String> typeName : typeMappings.keys()) {
                    MappingMetaData typeMapping = typeMappings.get(typeName.value);
                    Map<String, Map<String, String>> properties = getPropertiesFromTypeMapping(typeMapping);
                    if (properties == null) {
                        continue;
                    }

                    for (Map.Entry<String, Map<String, String>> propertyEntry : properties.entrySet()) {
                        String rawPropertyName = propertyEntry.getKey().replace(FIELDNAME_DOT_REPLACEMENT, ".");
                        loadExistingPropertyMappingIntoIndexInfo(graph, indexInfo, rawPropertyName);
                    }
                }
            }
        } catch (IOException ex) {
            throw new VertexiumException("Could not load type mappings", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, String>> getPropertiesFromTypeMapping(MappingMetaData typeMapping) throws IOException {
        return (Map<String, Map<String, String>>) typeMapping.getSourceAsMap().get("properties");
    }

    private void loadExistingPropertyMappingIntoIndexInfo(Graph graph, IndexInfo indexInfo, String rawPropertyName) {
        ElasticsearchPropertyNameInfo p = ElasticsearchPropertyNameInfo.parse(graph, propertyNameVisibilitiesStore, rawPropertyName);
        if (p == null) {
            return;
        }
        addPropertyNameVisibility(graph, indexInfo, rawPropertyName, p.getPropertyVisibility());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement: %s", element.getId());
        }

        if (!getConfig().isIndexEdges() && element instanceof Edge) {
            return;
        }

        if (flushObjectQueue.containsElementId(element.getId())) {
            flushObjectQueue.flush();
        }

        UpdateRequestBuilder updateRequestBuilder = prepareUpdate(graph, element, authorizations);
        addActionRequestBuilderForFlush(element.getId(), updateRequestBuilder);

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    @Override
    public <TElement extends Element> void updateElement(Graph graph, ExistingElementMutation<TElement> mutation, Authorizations authorizations) {
        TElement element = mutation.getElement();

        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("updateElement: %s", element.getId());
        }

        if (!getConfig().isIndexEdges() && element instanceof Edge) {
            return;
        }

        if (flushObjectQueue.containsElementId(element.getId())) {
            flushObjectQueue.flush();
        }

        UpdateRequestBuilder updateRequestBuilder = prepareUpdateForMutation(graph, mutation);

        if (updateRequestBuilder != null) {
            IndexInfo indexInfo = addMutationPropertiesToIndex(graph, mutation);
            getIndexRefreshTracker().pushChange(indexInfo.getIndexName());
            addActionRequestBuilderForFlush(element.getId(), updateRequestBuilder);

            if (getConfig().isAutoFlush()) {
                flush(graph);
            }
        }
    }

    private <TElement extends Element> UpdateRequestBuilder prepareUpdateForMutation(Graph graph, ExistingElementMutation<TElement> mutation) {
        TElement element = mutation.getElement();

        Map<String, String> fieldVisibilityChanges = getFieldVisibilityChanges(graph, mutation);
        List<String> fieldsToRemove = getFieldsToRemove(graph, mutation);
        Map<String, Object> fieldsToSet = getFieldsToSet(graph, mutation);

        String documentId = getIdStrategy().createElementDocId(element);
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
        return prepareUpdateFieldsOnDocument(indexInfo.getIndexName(), documentId, fieldsToSet, fieldsToRemove, fieldVisibilityChanges);
    }

    private <TElement extends Element> Map<String, Object> getFieldsToSet(Graph graph, ExistingElementMutation<TElement> mutation) {
        TElement element = mutation.getElement();

        Map<String, Object> fieldsToSet = new HashMap<>();

        mutation.getProperties().forEach(p ->
                addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));
        mutation.getPropertyDeletes().forEach(p ->
                addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));
        mutation.getPropertySoftDeletes().forEach(p ->
                addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));

        return fieldsToSet;
    }

    private <TElement extends Element> List<String> getFieldsToRemove(Graph graph, ExistingElementMutation<TElement> mutation) {
        List<String> fieldsToRemove = new ArrayList<>();
        mutation.getPropertyDeletes().forEach(p ->
                fieldsToRemove.add(addVisibilityToPropertyName(graph, p.getName(), p.getVisibility())));
        mutation.getPropertySoftDeletes().forEach(p ->
                fieldsToRemove.add(addVisibilityToPropertyName(graph, p.getName(), p.getVisibility())));
        return fieldsToRemove;
    }

    private <TElement extends Element> Map<String, String> getFieldVisibilityChanges(Graph graph, ExistingElementMutation<TElement> mutation) {
        Map<String, String> fieldVisibilityChanges = new HashMap<>();

        mutation.getAlterPropertyVisibilities().stream()
                .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
                .forEach(p -> {
                    String oldFieldName = addVisibilityToPropertyName(graph, p.getName(), p.getExistingVisibility());
                    String newFieldName = addVisibilityToPropertyName(graph, p.getName(), p.getVisibility());
                    fieldVisibilityChanges.put(oldFieldName, newFieldName);
                });

        if (mutation.getNewElementVisibility() != null) {
            String oldFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, mutation.getOldElementVisibility());
            String newFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, mutation.getNewElementVisibility());
            fieldVisibilityChanges.put(oldFieldName, newFieldName);
        }
        return fieldVisibilityChanges;
    }

    private void addExistingValuesToFieldMap(Graph graph, Element element, String propertyName, Visibility propertyVisibility, Map<String, Object> fieldsToSet) {
        Iterable<Property> properties = stream(element.getProperties(propertyName))
                .filter(p -> p.getVisibility().equals(propertyVisibility))
                .collect(Collectors.toList());
        Map<String, Object> remainingProperties = getPropertiesAsFields(graph, properties);
        for (Map.Entry<String, Object> remainingPropertyEntry : remainingProperties.entrySet()) {
            String remainingField = remainingPropertyEntry.getKey();
            Object remainingValue = remainingPropertyEntry.getValue();
            if (remainingValue instanceof List) {
                ((List) remainingValue).forEach(v -> addPropertyValueToPropertiesMap(fieldsToSet, remainingField, v));
            } else {
                addPropertyValueToPropertiesMap(fieldsToSet, remainingField, remainingValue);
            }
        }
    }

    private <TElement extends Element> IndexInfo addMutationPropertiesToIndex(Graph graph, ExistingElementMutation<TElement> mutation) {
        TElement element = mutation.getElement();
        IndexInfo indexInfo = addPropertiesToIndex(graph, element, mutation.getProperties());
        mutation.getAlterPropertyVisibilities().stream()
                .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
                .forEach(p -> {
                    PropertyDefinition propertyDefinition = getPropertyDefinition(graph, p.getName());
                    if (propertyDefinition != null) {
                        try {
                            addPropertyDefinitionToIndex(graph, indexInfo, p.getName(), p.getVisibility(), propertyDefinition);
                        } catch (Exception e) {
                            throw new VertexiumException("Unable to add property to index: " + p, e);
                        }
                    }
                });
        if (mutation.getNewElementVisibility() != null) {
            try {
                String newFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, mutation.getNewElementVisibility());
                addPropertyToIndex(graph, indexInfo, newFieldName, element.getVisibility(), String.class, false, false, false);
            } catch (Exception e) {
                throw new VertexiumException("Unable to add new element type visibility to index", e);
            }
        }
        return indexInfo;
    }

    private UpdateRequestBuilder prepareUpdate(Graph graph, Element element, Authorizations authorizations) {
        try {
            IndexInfo indexInfo = addPropertiesToIndex(graph, element, element.getProperties());
            XContentBuilder source = buildJsonContentFromElement(graph, element, authorizations);
            if (MUTATION_LOGGER.isTraceEnabled()) {
                MUTATION_LOGGER.trace("addElement json: %s: %s", element.getId(), source.string());
            }

            getConfig().getScoringStrategy().addElement(this, graph, element, authorizations);
            getIndexRefreshTracker().pushChange(indexInfo.getIndexName());
            return getClient()
                    .prepareUpdate(indexInfo.getIndexName(), getIdStrategy().getType(), getIdStrategy().createElementDocId(element))
                    .setDocAsUpsert(true)
                    .setDoc(source)
                    .setRetryOnConflict(MAX_RETRIES);
        } catch (IOException e) {
            throw new VertexiumException("Could not add element", e);
        }
    }

    private void addActionRequestBuilderForFlush(String elementId, UpdateRequestBuilder updateRequestBuilder) {
        addActionRequestBuilderForFlush(elementId, null, updateRequestBuilder);
    }

    private void addActionRequestBuilderForFlush(String elementId, String rowId, UpdateRequestBuilder updateRequestBuilder) {
        Future future;
        try {
            future = updateRequestBuilder.execute();
        } catch (Exception ex) {
            LOGGER.debug("Could not execute update: %s", ex.getMessage());
            future = SettableFuture.create();
            ((SettableFuture) future).setException(ex);
        }
        flushObjectQueue.add(elementId, rowId, updateRequestBuilder, future);
    }

    @Override
    public void addElementExtendedData(Graph graph, Element element, Iterable<ExtendedDataMutation> extendedData, Authorizations authorizations) {
        Map<String, Map<String, List<ExtendedDataMutation>>> extendedDataByTableByRow = mapExtendedDatasByTableByRow(extendedData);
        for (Map.Entry<String, Map<String, List<ExtendedDataMutation>>> byTable : extendedDataByTableByRow.entrySet()) {
            String tableName = byTable.getKey();
            Map<String, List<ExtendedDataMutation>> byRow = byTable.getValue();
            for (Map.Entry<String, List<ExtendedDataMutation>> row : byRow.entrySet()) {
                String rowId = row.getKey();
                List<ExtendedDataMutation> columns = row.getValue();
                addElementExtendedData(graph, element, tableName, rowId, columns, authorizations);
            }
        }
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId rowId, Authorizations authorizations) {
        String indexName = getExtendedDataIndexName(rowId);
        String docId = getIdStrategy().createExtendedDataDocId(rowId);
        getIndexRefreshTracker().pushChange(indexName);
        getClient().prepareDelete(indexName, getIdStrategy().getType(), docId).execute().actionGet();
    }

    @Override
    public void deleteExtendedData(
            Graph graph,
            Element element,
            String tableName,
            String row,
            String columnName,
            String key,
            Visibility visibility,
            Authorizations authorizations
    ) {
        String extendedDataDocId = getIdStrategy().createExtendedDataDocId(element, tableName, row);
        String fieldName = addVisibilityToPropertyName(graph, columnName, visibility);
        String indexName = getExtendedDataIndexName(element, tableName, row);
        removeFieldsFromDocument(graph, indexName, extendedDataDocId, Lists.newArrayList(fieldName, fieldName + "_e"));
    }

    private void addElementExtendedData(
            Graph graph,
            Element element,
            String tableName,
            String rowId,
            List<ExtendedDataMutation> columns,
            Authorizations authorizations
    ) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElementExtendedData: %s:%s:%s", element.getId(), tableName, rowId);
        }

        UpdateRequestBuilder updateRequestBuilder = prepareUpdate(graph, element, tableName, rowId, columns, authorizations);
        addActionRequestBuilderForFlush(element.getId(), rowId, updateRequestBuilder);

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    @Override
    public void addExtendedData(Graph graph, Iterable<ExtendedDataRow> extendedDatas, Authorizations authorizations) {
        Map<ElementType, Map<String, List<ExtendedDataRow>>> rowsByElementTypeAndId = mapExtendedDatasByElementTypeByElementId(extendedDatas);

        // prefetch the vertices and edges for performance
        Map<String, Vertex> verticesById;
        if (rowsByElementTypeAndId.containsKey(ElementType.VERTEX) && !rowsByElementTypeAndId.get(ElementType.VERTEX).isEmpty()) {
            Iterable<Vertex> vertices = graph.getVertices(rowsByElementTypeAndId.get(ElementType.VERTEX).keySet(), FetchHints.NONE, authorizations);
            verticesById = stream(vertices).collect(Collectors.toMap(Vertex::getId, Function.identity()));
        } else {
            verticesById = new HashMap<>();
        }
        Map<String, Edge> edgesById;
        if (rowsByElementTypeAndId.containsKey(ElementType.EDGE) && !rowsByElementTypeAndId.get(ElementType.EDGE).isEmpty()) {
            edgesById = stream(graph.getEdges(rowsByElementTypeAndId.get(ElementType.EDGE).keySet(), FetchHints.NONE, authorizations))
                    .collect(Collectors.toMap(Edge::getId, Function.identity()));
        } else {
            edgesById = new HashMap<>();
        }

        Set<String> missingElements = new HashSet<>();
        rowsByElementTypeAndId.forEach((elementType, elements) -> {
            elements.forEach((elementId, rows) -> {
                Element element = elementType == ElementType.VERTEX ? verticesById.get(elementId) : edgesById.get(elementId);
                if (element == null) {
                    missingElements.add(String.format(
                            "%s:%s",
                            elementType == ElementType.VERTEX ? "Vertex" : "Edge",
                            elementId
                    ));
                    return;
                }
                bulkUpdate(graph, new ConvertingIterable<ExtendedDataRow, UpdateRequest>(rows) {
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
                        return prepareUpdate(graph, element, tableName, rowId, columns, authorizations).request();
                    }
                });
            });
        });
        if (missingElements.size() > 0) {
            throw new VertexiumException("Could not add all extended data, missing elements: " + Joiner.on(", ").join(missingElements));
        }
    }

    private UpdateRequestBuilder prepareUpdate(
            Graph graph,
            Element element,
            String tableName,
            String rowId,
            List<ExtendedDataMutation> columns,
            Authorizations authorizations
    ) {
        try {
            IndexInfo indexInfo = addExtendedDataColumnsToIndex(graph, element, tableName, rowId, columns);
            XContentBuilder source = buildJsonContentFromExtendedDataMutations(graph, element, tableName, rowId, columns, authorizations).endObject();

            if (MUTATION_LOGGER.isTraceEnabled()) {
                MUTATION_LOGGER.trace("addElementExtendedData json: %s:%s:%s: %s", element.getId(), tableName, rowId, source.string());
            }

            getConfig().getScoringStrategy().addElementExtendedData(this, graph, element, tableName, rowId, columns, authorizations);

            String extendedDataDocId = getIdStrategy().createExtendedDataDocId(element, tableName, rowId);
            getIndexRefreshTracker().pushChange(indexInfo.getIndexName());
            return getClient()
                    .prepareUpdate(indexInfo.getIndexName(), getIdStrategy().getType(), extendedDataDocId)
                    .setDocAsUpsert(true)
                    .setDoc(source)
                    .setRetryOnConflict(MAX_RETRIES);
        } catch (IOException e) {
            throw new VertexiumException("Could not add element extended data", e);
        }
    }

    private XContentBuilder buildJsonContentFromExtendedDataMutations(
            Graph graph,
            Element element,
            String tableName,
            String rowId,
            List<ExtendedDataMutation> columns,
            Authorizations authorizations
    ) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        String elementTypeString = ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElement(element).getKey();
        jsonBuilder.field(ELEMENT_ID_FIELD_NAME, element.getId());
        jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, elementTypeString);
        String elementTypeVisibilityPropertyName = addElementTypeVisibilityPropertyToIndex(graph, element);
        jsonBuilder.field(elementTypeVisibilityPropertyName, elementTypeString);
        getConfig().getScoringStrategy().addFieldsToExtendedDataDocument(this, jsonBuilder, element, null, tableName, rowId, columns, authorizations);
        jsonBuilder.field(EXTENDED_DATA_TABLE_NAME_FIELD_NAME, tableName);
        jsonBuilder.field(EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME, rowId);
        if (element instanceof Edge) {
            Edge edge = (Edge) element;
            jsonBuilder.field(IN_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.IN));
            jsonBuilder.field(OUT_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.OUT));
            jsonBuilder.field(EDGE_LABEL_FIELD_NAME, edge.getLabel());
        }

        Map<String, Object> fields = getExtendedDataColumnsAsFields(graph, columns);
        addFieldsMap(jsonBuilder, fields);

        return jsonBuilder;
    }

    private Map<String, Object> getExtendedDataColumnsAsFields(Graph graph, List<ExtendedDataMutation> columns) {
        Map<String, Object> fieldsMap = new HashMap<>();
        List<ExtendedDataMutation> streamingColumns = new ArrayList<>();
        for (ExtendedDataMutation column : columns) {
            if (column.getValue() != null && shouldIgnoreType(column.getValue().getClass())) {
                continue;
            }

            if (column.getValue() instanceof StreamingPropertyValue) {
                StreamingPropertyValue spv = (StreamingPropertyValue) column.getValue();
                if (isStreamingPropertyValueIndexable(graph, column.getColumnName(), spv)) {
                    streamingColumns.add(column);
                }
            } else {
                addExtendedDataColumnToFieldMap(graph, column, column.getValue(), fieldsMap);
            }
        }
        addStreamingExtendedDataColumnsValuesToMap(graph, streamingColumns, fieldsMap);
        return fieldsMap;
    }

    private void addStreamingExtendedDataColumnsValuesToMap(Graph graph, List<ExtendedDataMutation> columns, Map<String, Object> fieldsMap) {
        List<StreamingPropertyValue> streamingPropertyValues = columns.stream()
                .map((column) -> {
                    if (!(column.getValue() instanceof StreamingPropertyValue)) {
                        throw new VertexiumException("column with a value that is not a StreamingPropertyValue passed to addStreamingPropertyValuesToFieldMap");
                    }
                    return (StreamingPropertyValue) column.getValue();
                })
                .collect(Collectors.toList());

        List<InputStream> inputStreams = graph.getStreamingPropertyValueInputStreams(streamingPropertyValues);
        for (int i = 0; i < columns.size(); i++) {
            try {
                String propertyValue = IOUtils.toString(inputStreams.get(i));
                addExtendedDataColumnToFieldMap(graph, columns.get(i), new StreamingPropertyString(propertyValue), fieldsMap);
            } catch (IOException ex) {
                throw new VertexiumException("could not convert streaming property to string", ex);
            }
        }
    }

    private void addExtendedDataColumnToFieldMap(Graph graph, ExtendedDataMutation column, Object value, Map<String, Object> fieldsMap) {
        String propertyName = addVisibilityToExtendedDataColumnName(graph, column);
        addValuesToFieldMap(graph, fieldsMap, propertyName, value);
    }

    private void addFieldsMap(XContentBuilder jsonBuilder, Map<String, Object> fields) throws IOException {
        for (Map.Entry<String, Object> property : fields.entrySet()) {
            String propertyKey = replaceFieldnameDots(property.getKey());
            if (property.getValue() instanceof List) {
                List list = (List) property.getValue();
                jsonBuilder.field(propertyKey, list.toArray(new Object[list.size()]));
            } else {
                jsonBuilder.field(propertyKey, property.getValue());
            }
        }
    }

    private Map<String, Map<String, List<ExtendedDataMutation>>> mapExtendedDatasByTableByRow(Iterable<ExtendedDataMutation> extendedData) {
        Map<String, Map<String, List<ExtendedDataMutation>>> results = new HashMap<>();
        for (ExtendedDataMutation ed : extendedData) {
            Map<String, List<ExtendedDataMutation>> byRow = results.computeIfAbsent(ed.getTableName(), k -> new HashMap<>());
            List<ExtendedDataMutation> items = byRow.computeIfAbsent(ed.getRow(), k -> new ArrayList<>());
            items.add(ed);
        }
        return results;
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

    @Override
    public void alterElementVisibility(
            Graph graph,
            Element element,
            Visibility oldVisibility,
            Visibility newVisibility,
            Authorizations authorizations
    ) {
        // Remove old element field name
        String oldFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, oldVisibility);
        removeFieldsFromDocument(graph, element, oldFieldName);

        addElement(graph, element, authorizations);
    }

    private XContentBuilder buildJsonContentFromElement(Graph graph, Element element, Authorizations authorizations) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        String elementTypeVisibilityPropertyName = addElementTypeVisibilityPropertyToIndex(graph, element);

        jsonBuilder.field(ELEMENT_ID_FIELD_NAME, element.getId());
        jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, getElementTypeValueFromElement(element));
        if (element instanceof Vertex) {
            jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.VERTEX.getKey());
            getConfig().getScoringStrategy().addFieldsToVertexDocument(this, jsonBuilder, (Vertex) element, null, authorizations);
        } else if (element instanceof Edge) {
            Edge edge = (Edge) element;
            jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.EDGE.getKey());
            getConfig().getScoringStrategy().addFieldsToEdgeDocument(this, jsonBuilder, edge, null, authorizations);
            jsonBuilder.field(IN_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.IN));
            jsonBuilder.field(OUT_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.OUT));
            jsonBuilder.field(EDGE_LABEL_FIELD_NAME, edge.getLabel());
        } else {
            throw new VertexiumException("Unexpected element type " + element.getClass().getName());
        }

        for (Visibility hiddenVisibility : element.getHiddenVisibilities()) {
            String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, hiddenVisibility);
            if (!isPropertyInIndex(graph, HIDDEN_VERTEX_FIELD_NAME, hiddenVisibility)) {
                String indexName = getIndexName(element);
                IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
                addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, hiddenVisibility, Boolean.class, false, false, false);
            }
            jsonBuilder.field(hiddenVisibilityPropertyName, true);
        }

        Map<String, Object> fields = getPropertiesAsFields(graph, element.getProperties());
        addFieldsMap(jsonBuilder, fields);

        jsonBuilder.endObject();
        return jsonBuilder;
    }

    @Override
    public void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        try {
            String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, visibility);
            String indexName = getIndexName(element);
            if (!isPropertyInIndex(graph, HIDDEN_VERTEX_FIELD_NAME, visibility)) {
                IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
                addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, visibility, Boolean.class, false, false, false);
            }

            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder().startObject();
            jsonBuilder.field(hiddenVisibilityPropertyName, true);
            jsonBuilder.endObject();

            getIndexRefreshTracker().pushChange(indexName);
            getClient()
                    .prepareUpdate(indexName, getIdStrategy().getType(), getIdStrategy().createElementDocId(element))
                    .setDoc(jsonBuilder)
                    .setRetryOnConflict(MAX_RETRIES)
                    .get();
        } catch (IOException e) {
            throw new VertexiumException("Could not mark element hidden", e);
        }
    }

    @Override
    public void markElementVisible(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, visibility);
        if (isPropertyInIndex(graph, HIDDEN_VERTEX_FIELD_NAME, visibility)) {
            removeFieldsFromDocument(graph, element, hiddenVisibilityPropertyName);
        }
    }

    @Override
    public void markPropertyHidden(Graph graph, Element element, Property property, Visibility visibility, Authorizations authorizations) {
        try {
            String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility);
            if (!isPropertyInIndex(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility)) {
                String indexName = getIndexName(element);
                IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
                addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, visibility, Boolean.class, false, false, false);
            }

            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder().startObject();
            jsonBuilder.field(hiddenVisibilityPropertyName, true);
            jsonBuilder.endObject();

            getClient()
                    .prepareUpdate(getIndexName(element), getIdStrategy().getType(), getIdStrategy().createElementDocId(element))
                    .setDoc(jsonBuilder)
                    .setRetryOnConflict(MAX_RETRIES)
                    .get();
        } catch (IOException e) {
            throw new VertexiumException("Could not mark element hidden", e);
        }
    }

    @Override
    public void markPropertyVisible(Graph graph, Element element, Property property, Visibility visibility, Authorizations authorizations) {
        String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility);
        if (isPropertyInIndex(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility)) {
            removeFieldsFromDocument(graph, element, hiddenVisibilityPropertyName);
        }
    }

    private String getElementTypeValueFromElement(Element element) {
        if (element instanceof Vertex) {
            return ElasticsearchDocumentType.VERTEX.getKey();
        }
        if (element instanceof Edge) {
            return ElasticsearchDocumentType.EDGE.getKey();
        }
        throw new VertexiumException("Unhandled element type: " + element.getClass().getName());
    }

    protected Object convertValueForIndexing(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        }
        if (obj instanceof BigInteger) {
            return ((BigInteger) obj).intValue();
        }
        return obj;
    }

    private String addElementTypeVisibilityPropertyToIndex(Graph graph, Element element) throws IOException {
        String elementTypeVisibilityPropertyName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, element.getVisibility());
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
        addPropertyToIndex(graph, indexInfo, elementTypeVisibilityPropertyName, element.getVisibility(), String.class, false, false, false);
        return elementTypeVisibilityPropertyName;
    }

    private Map<String, Object> getPropertiesAsFields(Graph graph, Iterable<Property> properties) {
        Map<String, Object> fieldsMap = new HashMap<>();
        List<Property> streamingProperties = new ArrayList<>();
        for (Property property : properties) {
            if (property.getValue() != null && shouldIgnoreType(property.getValue().getClass())) {
                continue;
            }

            if (property.getValue() instanceof StreamingPropertyValue) {
                StreamingPropertyValue spv = (StreamingPropertyValue) property.getValue();
                if (isStreamingPropertyValueIndexable(graph, property.getName(), spv)) {
                    streamingProperties.add(property);
                }
            } else {
                addPropertyToFieldMap(graph, property, property.getValue(), fieldsMap);
            }
        }
        addStreamingPropertyValuesToFieldMap(graph, streamingProperties, fieldsMap);
        return fieldsMap;
    }

    private void addPropertyToFieldMap(Graph graph, Property property, Object propertyValue, Map<String, Object> propertiesMap) {
        String propertyName = addVisibilityToPropertyName(graph, property);
        addValuesToFieldMap(graph, propertiesMap, propertyName, propertyValue);
    }

    private void addValuesToFieldMap(Graph graph, Map<String, Object> propertiesMap, String propertyName, Object propertyValue) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);
        if (propertyValue instanceof GeoShape) {
            convertGeoShape(propertiesMap, propertyName, (GeoShape) propertyValue);
            if (propertyValue instanceof GeoPoint) {
                GeoPoint geoPoint = (GeoPoint) propertyValue;
                List<Double> coordinates = Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude());
                addPropertyValueToPropertiesMap(propertiesMap, propertyName + GEO_POINT_PROPERTY_NAME_SUFFIX, coordinates);
            }
            return;
        } else if (propertyValue instanceof StreamingPropertyString) {
            propertyValue = ((StreamingPropertyString) propertyValue).getPropertyValue();
        } else if (propertyValue instanceof String) {
            if (propertyDefinition == null ||
                    propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT) ||
                    propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH) ||
                    propertyDefinition.isSortable()) {
                addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
            }
            return;
        }

        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }

        addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
    }

    private boolean isStreamingPropertyValueIndexable(Graph graph, String propertyName, StreamingPropertyValue streamingPropertyValue) {
        if (!streamingPropertyValue.isSearchIndex()) {
            return false;
        }

        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);
        if (propertyDefinition != null && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
            return false;
        }

        Class valueType = streamingPropertyValue.getValueType();
        if (valueType == String.class) {
            return true;
        } else {
            throw new VertexiumException("Unhandled StreamingPropertyValue type: " + valueType.getName());
        }
    }

    private void addStreamingPropertyValuesToFieldMap(Graph graph, List<Property> properties, Map<String, Object> propertiesMap) {
        List<StreamingPropertyValue> streamingPropertyValues = properties.stream()
                .map((property) -> {
                    if (!(property.getValue() instanceof StreamingPropertyValue)) {
                        throw new VertexiumException("property with a value that is not a StreamingPropertyValue passed to addStreamingPropertyValuesToFieldMap");
                    }
                    return (StreamingPropertyValue) property.getValue();
                })
                .collect(Collectors.toList());
        if (streamingPropertyValues.size() > 0 && graph instanceof GraphWithSearchIndex) {
            ((GraphWithSearchIndex) graph).flushGraph();
        }

        List<InputStream> inputStreams = graph.getStreamingPropertyValueInputStreams(streamingPropertyValues);
        for (int i = 0; i < properties.size(); i++) {
            try {
                String propertyValue = IOUtils.toString(inputStreams.get(i));
                addPropertyToFieldMap(graph, properties.get(i), new StreamingPropertyString(propertyValue), propertiesMap);
            } catch (IOException ex) {
                throw new VertexiumException("could not convert streaming property to string", ex);
            }
        }
    }

    public boolean isServerPluginInstalled() {
        return serverPluginInstalled;
    }

    private static class StreamingPropertyString {
        private final String propertyValue;

        public StreamingPropertyString(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        public String getPropertyValue() {
            return propertyValue;
        }
    }

    protected String addVisibilityToPropertyName(Graph graph, Property property) {
        String propertyName = property.getName();
        Visibility propertyVisibility = property.getVisibility();
        return addVisibilityToPropertyName(graph, propertyName, propertyVisibility);
    }

    protected String addVisibilityToExtendedDataColumnName(Graph graph, ExtendedDataMutation extendedDataMutation) {
        String columnName = extendedDataMutation.getColumnName();
        Visibility propertyVisibility = extendedDataMutation.getVisibility();
        return addVisibilityToPropertyName(graph, columnName, propertyVisibility);
    }

    String addVisibilityToPropertyName(Graph graph, String propertyName, Visibility propertyVisibility) {
        String visibilityHash = getVisibilityHash(graph, propertyName, propertyVisibility);
        return propertyName + "_" + visibilityHash;
    }

    protected String removeVisibilityFromPropertyName(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            string = m.group(1);
        }
        return string;
    }

    private String removeVisibilityFromPropertyNameWithTypeSuffix(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            if (m.groupCount() >= 4 && m.group(4) != null) {
                string = m.group(1) + m.group(4);
            } else {
                string = m.group(1);
            }
        }
        return string;
    }

    public String getPropertyVisibilityHashFromPropertyName(String propertyName) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(propertyName);
        if (m.matches()) {
            return m.group(3);
        }
        throw new VertexiumException("Could not match property name: " + propertyName);
    }

    public String getAggregationName(String name) {
        Matcher m = AGGREGATION_NAME_PATTERN.matcher(name);
        if (m.matches()) {
            return m.group(1);
        }
        throw new VertexiumException("Could not get aggregation name from: " + name);
    }

    public String replaceFieldnameDots(String fieldName) {
        return fieldName.replace(".", FIELDNAME_DOT_REPLACEMENT);
    }

    public String[] getAllMatchingPropertyNames(Graph graph, String propertyName, Authorizations authorizations) {
        if (Edge.LABEL_PROPERTY_NAME.equals(propertyName)
                || Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
                || Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return new String[]{propertyName};
        }
        Collection<String> hashes = this.propertyNameVisibilitiesStore.getHashes(graph, propertyName, authorizations);
        return addHashesToPropertyName(propertyName, hashes);
    }

    public String[] addHashesToPropertyName(String propertyName, Collection<String> hashes) {
        if (hashes.size() == 0) {
            return new String[0];
        }
        String[] results = new String[hashes.size()];
        int i = 0;
        for (String hash : hashes) {
            results[i++] = propertyName + "_" + hash;
        }
        return results;
    }

    public Collection<String> getQueryableElementTypeVisibilityPropertyNames(Graph graph, Authorizations authorizations) {
        Set<String> propertyNames = new HashSet<>();
        for (String hash : propertyNameVisibilitiesStore.getHashes(graph, ELEMENT_TYPE_FIELD_NAME, authorizations)) {
            propertyNames.add(ELEMENT_TYPE_FIELD_NAME + "_" + hash);
        }
        if (propertyNames.size() == 0) {
            throw new VertexiumNoMatchingPropertiesException("No queryable " + ELEMENT_TYPE_FIELD_NAME + " for authorizations " + authorizations);
        }
        return propertyNames;
    }

    public Collection<String> getQueryablePropertyNames(Graph graph, Authorizations authorizations) {
        Set<String> propertyNames = new HashSet<>();
        for (PropertyDefinition propertyDefinition : graph.getPropertyDefinitions()) {
            List<String> queryableTypeSuffixes = getQueryableTypeSuffixes(propertyDefinition);
            if (queryableTypeSuffixes.size() == 0) {
                continue;
            }
            String propertyNameNoVisibility = removeVisibilityFromPropertyName(propertyDefinition.getPropertyName()); // could have visibility
            if (isReservedFieldName(propertyNameNoVisibility)) {
                continue;
            }
            for (String hash : propertyNameVisibilitiesStore.getHashes(graph, propertyNameNoVisibility, authorizations)) {
                for (String typeSuffix : queryableTypeSuffixes) {
                    propertyNames.add(propertyNameNoVisibility + "_" + hash + typeSuffix);
                }
            }
        }
        return propertyNames;
    }

    private static List<String> getQueryableTypeSuffixes(PropertyDefinition propertyDefinition) {
        List<String> typeSuffixes = new ArrayList<>();
        if (propertyDefinition.getDataType() == String.class) {
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                typeSuffixes.add(EXACT_MATCH_PROPERTY_NAME_SUFFIX);
            }
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                typeSuffixes.add("");
            }
        } else if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
            typeSuffixes.add("");
        }
        return typeSuffixes;
    }

    protected static boolean isReservedFieldName(String fieldName) {
        return fieldName.startsWith("__");
    }

    private String getVisibilityHash(Graph graph, String propertyName, Visibility visibility) {
        return this.propertyNameVisibilitiesStore.getHash(graph, propertyName, visibility);
    }

    @Override
    public void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        deleteExtendedDataForElement(element);

        String indexName = getIndexName(element);
        String docId = getIdStrategy().createElementDocId(element);
        if (MUTATION_LOGGER.isTraceEnabled()) {
            LOGGER.trace("deleting document %s (docId: %s)", element.getId(), docId);
        }
        getIndexRefreshTracker().pushChange(indexName);
        getClient().delete(
                getClient()
                        .prepareDelete(indexName, getIdStrategy().getType(), docId)
                        .request()
        ).actionGet();
    }

    private void deleteExtendedDataForElement(Element element) {
        try {
            QueryBuilder filter = QueryBuilders.termQuery(ELEMENT_ID_FIELD_NAME, element.getId());

            SearchRequestBuilder s = getClient().prepareSearch(getIndicesToQuery())
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
                getIndexRefreshTracker().pushChange(hit.getIndex());
                getClient().prepareDelete(hit.getIndex(), hit.getType(), hit.getId()).execute().actionGet();
            }
        } catch (Exception ex) {
            throw new VertexiumException("Could not delete extended data for element: " + element.getId());
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
                queryString,
                new ElasticsearchSearchQueryBase.Options()
                        .setScoringStrategy(getConfig().getScoringStrategy())
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize()),
                authorizations
        );
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchVertexQuery(
                getClient(),
                graph,
                vertex,
                queryString,
                new ElasticsearchSearchVertexQuery.Options()
                        .setScoringStrategy(getConfig().getScoringStrategy())
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize()),
                authorizations
        );
    }

    @Override
    public Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchExtendedDataQuery(
                getClient(),
                graph,
                element.getId(),
                tableName,
                queryString,
                new ElasticsearchSearchExtendedDataQuery.Options()
                        .setScoringStrategy(getConfig().getScoringStrategy())
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize()),
                authorizations
        );
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticsearchSearchGraphQuery(
                getClient(),
                graph,
                similarToFields,
                similarToText,
                new ElasticsearchSearchQueryBase.Options()
                        .setScoringStrategy(getConfig().getScoringStrategy())
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize()),
                authorizations
        );
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    protected void addPropertyDefinitionToIndex(Graph graph, IndexInfo indexInfo, String propertyName, Visibility propertyVisibility, PropertyDefinition propertyDefinition) throws IOException {
        String propertyNameWithVisibility = addVisibilityToPropertyName(graph, propertyName, propertyVisibility);

        if (propertyDefinition.getDataType() == String.class) {
            boolean exact = propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH);
            boolean analyzed = propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT);
            boolean sortable = propertyDefinition.isSortable();
            if (analyzed || exact || sortable) {
                addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, String.class, analyzed, exact, sortable);
            }
            return;
        }

        if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyDefinition.getDataType(), true, false, false);
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, String.class, true, true, false);
            if (propertyDefinition.getDataType() == GeoPoint.class) {
                addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_POINT_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyDefinition.getDataType(), true, true, false);
            }
            return;
        }

        addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, propertyDefinition.getDataType(), true, false, false);
    }

    protected PropertyDefinition getPropertyDefinition(Graph graph, String propertyName) {
        propertyName = removeVisibilityFromPropertyNameWithTypeSuffix(propertyName);
        return graph.getPropertyDefinition(propertyName);
    }

    public void addPropertyToIndex(Graph graph, IndexInfo indexInfo, String propertyName, Object propertyValue, Visibility propertyVisibility) throws IOException {
        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);
        if (propertyDefinition != null) {
            addPropertyDefinitionToIndex(graph, indexInfo, propertyName, propertyVisibility, propertyDefinition);
        } else {
            addPropertyToIndexInner(graph, indexInfo, propertyName, propertyValue, propertyVisibility);
        }

        propertyDefinition = getPropertyDefinition(graph, propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX);
        if (propertyDefinition != null) {
            addPropertyDefinitionToIndex(graph, indexInfo, propertyName, propertyVisibility, propertyDefinition);
        }

        if (propertyValue instanceof GeoShape) {
            propertyDefinition = getPropertyDefinition(graph, propertyName + GEO_PROPERTY_NAME_SUFFIX);
            if (propertyDefinition != null) {
                addPropertyDefinitionToIndex(graph, indexInfo, propertyName, propertyVisibility, propertyDefinition);
            }
        }
    }

    private void addPropertyToIndexInner(Graph graph, IndexInfo indexInfo, String propertyName, Object propertyValue, Visibility propertyVisibility) throws IOException {
        String propertyNameWithVisibility = addVisibilityToPropertyName(graph, propertyName, propertyVisibility);

        if (indexInfo.isPropertyDefined(propertyNameWithVisibility, propertyVisibility)) {
            return;
        }

        Class dataType;
        if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return;
            }
            dataType = streamingPropertyValue.getValueType();
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, false, false);
        } else if (propertyValue instanceof String) {
            dataType = String.class;
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, true, false);
        } else if (propertyValue instanceof GeoShape) {
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyValue.getClass(), true, false, false);
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, String.class, true, true, false);
            if (propertyValue instanceof GeoPoint) {
                addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_POINT_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyValue.getClass(), true, true, false);
            }
        } else {
            checkNotNull(propertyValue, "property value cannot be null for property: " + propertyNameWithVisibility);
            dataType = propertyValue.getClass();
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, false, false);
        }
    }

    protected void addPropertyToIndex(
            Graph graph,
            IndexInfo indexInfo,
            String propertyName,
            Visibility propertyVisibility,
            Class dataType,
            boolean analyzed,
            boolean exact,
            boolean sortable
    ) throws IOException {
        if (indexInfo.isPropertyDefined(propertyName, propertyVisibility)) {
            return;
        }

        if (shouldIgnoreType(dataType)) {
            return;
        }

        this.indexInfosLock.writeLock().lock();
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(getIdStrategy().getType())
                    .startObject("properties")
                    .startObject(replaceFieldnameDots(propertyName));

            addTypeToMapping(mapping, propertyName, dataType, analyzed, exact, sortable);

            mapping
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("addPropertyToIndex: %s: %s", dataType.getName(), mapping.string());
            }

            getClient()
                    .admin()
                    .indices()
                    .preparePutMapping(indexInfo.getIndexName())
                    .setType(getIdStrategy().getType())
                    .setSource(mapping)
                    .execute()
                    .actionGet();

            addPropertyNameVisibility(graph, indexInfo, propertyName, propertyVisibility);
            updateMetadata(graph, indexInfo);
        } finally {
            this.indexInfosLock.writeLock().unlock();
        }
    }

    private void updateMetadata(Graph graph, IndexInfo indexInfo) {
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(getIdStrategy().getType());
            GetMappingsResponse existingMapping = getClient()
                    .admin()
                    .indices()
                    .prepareGetMappings(indexInfo.getIndexName())
                    .execute()
                    .actionGet();

            Map<String, Object> existingElementData = existingMapping.mappings()
                    .get(indexInfo.getIndexName())
                    .get(getIdStrategy().getType())
                    .getSourceAsMap();

            mapping = mapping.startObject("_meta")
                    .startObject("vertexium");
            //noinspection unchecked
            Map<String, Object> properties = (Map<String, Object>) existingElementData.get("properties");
            for (String propertyName : properties.keySet()) {
                ElasticsearchPropertyNameInfo p = ElasticsearchPropertyNameInfo.parse(graph, propertyNameVisibilitiesStore, propertyName);
                if (p == null || p.getPropertyVisibility() == null) {
                    continue;
                }
                mapping.field(replaceFieldnameDots(propertyName), p.getPropertyVisibility());
            }
            mapping.endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            getClient()
                    .admin()
                    .indices()
                    .preparePutMapping(indexInfo.getIndexName())
                    .setType(getIdStrategy().getType())
                    .setSource(mapping)
                    .execute()
                    .actionGet();
        } catch (IOException ex) {
            throw new VertexiumException("Could not update mapping", ex);
        }
    }

    protected void addPropertyNameVisibility(Graph graph, IndexInfo indexInfo, String propertyName, Visibility propertyVisibility) {
        String propertyNameNoVisibility = removeVisibilityFromPropertyName(propertyName);
        if (propertyVisibility != null) {
            this.propertyNameVisibilitiesStore.getHash(graph, propertyNameNoVisibility, propertyVisibility);
        }
        indexInfo.addPropertyNameVisibility(propertyNameNoVisibility, propertyVisibility);
        indexInfo.addPropertyNameVisibility(propertyName, propertyVisibility);
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(Graph graph, String propertyName, Authorizations authorizations) {
        TermQueryBuilder elementTypeFilterBuilder = new TermQueryBuilder(ELEMENT_TYPE_FIELD_NAME, ElasticsearchDocumentType.VERTEX.getKey());
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
                .filter(elementTypeFilterBuilder);
        SearchRequestBuilder q = getClient().prepareSearch(getIndexNamesAsArray(graph))
                .setQuery(queryBuilder)
                .setSize(0);

        for (String p : getAllMatchingPropertyNames(graph, propertyName, authorizations)) {
            String countAggName = "count-" + p;
            PropertyDefinition propertyDefinition = getPropertyDefinition(graph, p);
            p = replaceFieldnameDots(p);
            if (propertyDefinition != null && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                p = p + EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }

            TermsAggregationBuilder countAgg = AggregationBuilders
                    .terms(countAggName)
                    .field(p)
                    .size(500000);
            q = q.addAggregation(countAgg);
        }

        if (ElasticsearchSearchQueryBase.QUERY_LOGGER.isTraceEnabled()) {
            ElasticsearchSearchQueryBase.QUERY_LOGGER.trace("query: %s", q);
        }
        SearchResponse response = checkForFailures(getClient().search(q.request()).actionGet());
        Map<Object, Long> results = new HashMap<>();
        for (Aggregation agg : response.getAggregations().asList()) {
            Terms propertyCountResults = (Terms) agg;
            for (Terms.Bucket propertyCountResult : propertyCountResults.getBuckets()) {
                String mapKey = ((String) propertyCountResult.getKey()).toLowerCase();
                Long previousValue = results.get(mapKey);
                if (previousValue == null) {
                    previousValue = 0L;
                }
                results.put(mapKey, previousValue + propertyCountResult.getDocCount());
            }
        }
        return results;
    }

    protected IndexInfo ensureIndexCreatedAndInitialized(Graph graph, String indexName) {
        Map<String, IndexInfo> indexInfos = getIndexInfos(graph);
        IndexInfo indexInfo = indexInfos.get(indexName);
        if (indexInfo != null && indexInfo.isElementTypeDefined()) {
            return indexInfo;
        }
        return initializeIndex(graph, indexInfo, indexName);
    }

    private IndexInfo initializeIndex(Graph graph, String indexName) {
        return initializeIndex(graph, null, indexName);
    }

    private IndexInfo initializeIndex(Graph graph, IndexInfo indexInfo, String indexName) {
        indexInfosLock.writeLock().lock();
        try {
            if (indexInfo == null) {
                if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
                    try {
                        createIndex(indexName);
                    } catch (Exception e) {
                        throw new VertexiumException("Could not create index: " + indexName, e);
                    }
                }

                indexInfo = createIndexInfo(indexName);

                if (indexInfos == null) {
                    loadIndexInfos(graph);
                }
                indexInfos.put(indexName, indexInfo);
            }

            ensureMappingsCreated(indexInfo);

            return indexInfo;
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }


    protected IndexInfo createIndexInfo(String indexName) {
        return new IndexInfo(indexName);
    }

    protected void ensureMappingsCreated(IndexInfo indexInfo) {
        if (!indexInfo.isElementTypeDefined()) {
            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_source").field("enabled", true).endObject()
                        .startObject("_all").field("enabled", isAllFieldEnabled()).endObject()
                        .startObject("properties");
                createIndexAddFieldsToElementType(mappingBuilder);
                XContentBuilder mapping = mappingBuilder.endObject()
                        .endObject();

                client.admin().indices().preparePutMapping(indexInfo.getIndexName())
                        .setType(getIdStrategy().getType())
                        .setSource(mapping)
                        .execute()
                        .actionGet();
                indexInfo.setElementTypeDefined(true);
            } catch (Throwable e) {
                throw new VertexiumException("Could not add mappings to index: " + indexInfo.getIndexName(), e);
            }
        }
    }

    protected void createIndexAddFieldsToElementType(XContentBuilder builder) throws IOException {
        builder
                .startObject(ELEMENT_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(ELEMENT_TYPE_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(EXTENDED_DATA_TABLE_NAME_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(VISIBILITY_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(IN_VERTEX_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(OUT_VERTEX_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(EDGE_LABEL_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
        ;
        getConfig().getScoringStrategy().addFieldsToElementType(builder);
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, Authorizations authorizations) {
        deleteProperties(graph, element, Collections.singletonList(property), authorizations);
    }

    @Override
    public void deleteProperties(Graph graph, Element element, Collection<PropertyDescriptor> propertyList, Authorizations authorizations) {
        List<String> fieldsToRemove = new ArrayList<>();
        Map<String, Object> fieldsToSet = new HashMap<>();
        propertyList.forEach(p -> {
            String fieldName = addVisibilityToPropertyName(graph, p.getName(), p.getVisibility());
            fieldsToRemove.add(fieldName);
            addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet);
        });

        String documentId = getIdStrategy().createElementDocId(element);
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
        UpdateRequestBuilder updateRequestBuilder = prepareUpdateFieldsOnDocument(indexInfo.getIndexName(), documentId, fieldsToSet, fieldsToRemove, Collections.emptyMap());
        if (updateRequestBuilder != null) {
            getIndexRefreshTracker().pushChange(indexInfo.getIndexName());
            addActionRequestBuilderForFlush(element.getId(), updateRequestBuilder);

            if (getConfig().isAutoFlush()) {
                flush(graph);
            }
        }
    }

    @Override
    public void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations) {
        bulkUpdate(graph, new ConvertingIterable<Element, UpdateRequest>(elements) {
            @Override
            protected UpdateRequest convert(Element element) {
                return prepareUpdate(graph, element, authorizations).request();
            }
        });
    }

    private void bulkUpdate(Graph graph, Iterable<UpdateRequest> updateRequests) {
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
            throw new VertexiumException(String.format("Failed bulk update (failures: %s)", failures.size()));
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
                vertexIds,
                queryString,
                new ElasticsearchSearchQueryBase.Options()
                        .setScoringStrategy(getConfig().getScoringStrategy())
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize()),
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
        getIndexRefreshTracker().refresh(client);
    }

    private void removeFieldsFromDocument(Graph graph, Element element, String field) {
        removeFieldsFromDocument(graph, element, Lists.newArrayList(field));
    }

    private void removeFieldsFromDocument(Graph graph, Element element, Collection<String> fields) {
        String indexName = getIndexName(element);
        String documentId = getIdStrategy().createElementDocId(element);
        removeFieldsFromDocument(graph, indexName, documentId, fields);
    }

    private void removeFieldsFromDocument(Graph graph, String indexName, String documentId, Collection<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return;
        }

        getIndexRefreshTracker().pushChange(indexName);
        UpdateRequestBuilder updateRequestBuilder = prepareRemoveFieldsFromDocument(indexName, documentId, fields);
        addActionRequestBuilderForFlush(documentId, updateRequestBuilder);

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    private UpdateRequestBuilder prepareRemoveFieldsFromDocument(String indexName, String documentId, Collection<String> fields) {
        List<String> fieldNames = fields.stream().map(this::replaceFieldnameDots).collect(Collectors.toList());
        if (fieldNames.isEmpty()) {
            return null;
        }

        return getClient().prepareUpdate()
                .setIndex(indexName)
                .setId(documentId)
                .setType(getIdStrategy().getType())
                .setScript(new Script(
                        ScriptType.INLINE,
                        "painless",
                        "for (def fieldName : params.fieldNames) { ctx._source.remove(fieldName); }",
                        ImmutableMap.of("fieldNames", fieldNames)
                ))
                .setRetryOnConflict(MAX_RETRIES);
    }

    private UpdateRequestBuilder prepareUpdateFieldsOnDocument(
            String indexName,
            String documentId,
            Map<String, Object> fieldsToSet,
            Collection<String> fieldsToRemove,
            Map<String, String> fieldsToRename
    ) {
        if ((fieldsToSet == null || fieldsToSet.isEmpty()) &&
                (fieldsToRemove == null || fieldsToRemove.isEmpty()) &&
                (fieldsToRename == null || fieldsToRename.isEmpty())) {
            return null;
        }

        fieldsToSet = fieldsToSet == null ? Collections.emptyMap() : fieldsToSet.entrySet().stream()
                .collect(Collectors.toMap(e -> replaceFieldnameDots(e.getKey()), Map.Entry::getValue));
        fieldsToRemove = fieldsToRemove == null ? Collections.emptyList() : fieldsToRemove.stream().map(this::replaceFieldnameDots).collect(Collectors.toList());
        fieldsToRename = fieldsToRename == null ? Collections.emptyMap() : fieldsToRename.entrySet().stream()
                .collect(Collectors.toMap(e -> replaceFieldnameDots(e.getKey()), e -> replaceFieldnameDots(e.getValue())));

        return getClient().prepareUpdate()
                .setIndex(indexName)
                .setId(documentId)
                .setType(getIdStrategy().getType())
                .setScript(new Script(
                        ScriptType.INLINE,
                        "painless",
                        "for (def fieldName : params.fieldsToRemove) { ctx._source.remove(fieldName); } " +
                                "for (fieldToRename in params.fieldsToRename.entrySet()) { ctx._source[fieldToRename.getValue()] = ctx._source[fieldToRename.getKey()]; ctx._source.remove(fieldToRename.getKey()); } " +
                                "for (fieldToSet in params.fieldsToSet.entrySet()) { ctx._source[fieldToSet.getKey()] = fieldToSet.getValue(); }",
                        ImmutableMap.of("fieldsToSet", fieldsToSet, "fieldsToRemove", fieldsToRemove, "fieldsToRename", fieldsToRename)
                ))
                .setRetryOnConflict(MAX_RETRIES);
    }

    protected String[] getIndexNamesAsArray(Graph graph) {
        Map<String, IndexInfo> indexInfos = getIndexInfos(graph);
        if (indexInfos.size() == indexInfosLastSize) {
            return indexNamesAsArray;
        }
        synchronized (this) {
            Set<String> keys = indexInfos.keySet();
            indexNamesAsArray = keys.toArray(new String[keys.size()]);
            indexInfosLastSize = indexInfos.size();
            return indexNamesAsArray;
        }
    }

    @Override
    public void shutdown() {
        client.close();

        if (inProcessNode != null) {
            try {
                inProcessNode.close();
            } catch (IOException ex) {
                LOGGER.error("could not close in process node", ex);
            }
            inProcessNode = null;
        }

        if (propertyNameVisibilitiesStore instanceof Closeable) {
            try {
                ((Closeable) propertyNameVisibilitiesStore).close();
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
    }

    @SuppressWarnings("unused")
    protected String[] getIndexNames(PropertyDefinition propertyDefinition) {
        return indexSelectionStrategy.getIndexNames(this, propertyDefinition);
    }

    protected String getIndexName(Element element) {
        return indexSelectionStrategy.getIndexName(this, element);
    }

    protected String getExtendedDataIndexName(Element element, String tableName, String rowId) {
        return indexSelectionStrategy.getExtendedDataIndexName(this, element, tableName, rowId);
    }

    protected String getExtendedDataIndexName(ExtendedDataRowId rowId) {
        return indexSelectionStrategy.getExtendedDataIndexName(this, rowId);
    }

    protected String[] getIndicesToQuery() {
        return indexSelectionStrategy.getIndicesToQuery(this);
    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    private IndexInfo addExtendedDataColumnsToIndex(Graph graph, Element element, String tableName, String rowId, List<ExtendedDataMutation> columns) {
        try {
            String indexName = getExtendedDataIndexName(element, tableName, rowId);
            IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
            for (ExtendedDataMutation column : columns) {
                addPropertyToIndex(graph, indexInfo, column.getColumnName(), column.getValue(), column.getVisibility());
            }
            return indexInfo;
        } catch (IOException e) {
            throw new VertexiumException("Could not add properties to index", e);
        }
    }

    public IndexInfo addPropertiesToIndex(Graph graph, Element element, Iterable<Property> properties) {
        try {
            String indexName = getIndexName(element);
            IndexInfo indexInfo = ensureIndexCreatedAndInitialized(graph, indexName);
            for (Property property : properties) {
                addPropertyToIndex(graph, indexInfo, property.getName(), property.getValue(), property.getVisibility());
            }
            return indexInfo;
        } catch (IOException e) {
            throw new VertexiumException("Could not add properties to index", e);
        }
    }

    protected boolean shouldIgnoreType(Class dataType) {
        return dataType == byte[].class;
    }

    protected void addTypeToMapping(XContentBuilder mapping, String propertyName, Class dataType, boolean analyzed, boolean exact, boolean sortable) throws IOException {
        if (dataType == String.class) {
            LOGGER.debug("Registering 'string' type for %s", propertyName);
            if (analyzed || exact || sortable) {
                mapping.field("type", "text");
                if (!analyzed) {
                    mapping.field("index", "false");
                }
                if (exact || sortable) {
                    mapping.startObject("fields");
                    mapping.startObject(EXACT_MATCH_FIELD_NAME)
                            .field("type", "keyword")
                            .field("ignore_above", EXACT_MATCH_IGNORE_ABOVE_LIMIT)
                            .field("normalizer", LOWERCASER_NORMALIZER_NAME)
                            .endObject();
                    mapping.endObject();
                }
            } else {
                mapping.field("type", "keyword");
                mapping.field("ignore_above", EXACT_MATCH_IGNORE_ABOVE_LIMIT);
                mapping.field("normalizer", LOWERCASER_NORMALIZER_NAME);
            }
        } else if (dataType == IpV4Address.class) {
            LOGGER.debug("Registering 'ip' type for %s", propertyName);
            mapping.field("type", "ip");
        } else if (dataType == Float.class || dataType == Float.TYPE) {
            LOGGER.debug("Registering 'float' type for %s", propertyName);
            mapping.field("type", "float");
        } else if (dataType == Double.class || dataType == Double.TYPE) {
            LOGGER.debug("Registering 'double' type for %s", propertyName);
            mapping.field("type", "double");
        } else if (dataType == Byte.class || dataType == Byte.TYPE) {
            LOGGER.debug("Registering 'byte' type for %s", propertyName);
            mapping.field("type", "byte");
        } else if (dataType == Short.class || dataType == Short.TYPE) {
            LOGGER.debug("Registering 'short' type for %s", propertyName);
            mapping.field("type", "short");
        } else if (dataType == Integer.class || dataType == Integer.TYPE) {
            LOGGER.debug("Registering 'integer' type for %s", propertyName);
            mapping.field("type", "integer");
        } else if (dataType == Long.class || dataType == Long.TYPE) {
            LOGGER.debug("Registering 'long' type for %s", propertyName);
            mapping.field("type", "long");
        } else if (dataType == Date.class || dataType == DateOnly.class) {
            LOGGER.debug("Registering 'date' type for %s", propertyName);
            mapping.field("type", "date");
        } else if (dataType == Boolean.class || dataType == Boolean.TYPE) {
            LOGGER.debug("Registering 'boolean' type for %s", propertyName);
            mapping.field("type", "boolean");
        } else if (dataType == GeoPoint.class && exact) {
            // ES5 doesn't support geo hash aggregations for shapes, so if this is a point marked for EXACT_MATCH
            // define it as a geo_point instead of a geo_shape. Points end up with 3 fields in the index for this
            // reason. This one for aggregating as well as the "_g" and description fields that all geoshapes get.
            LOGGER.debug("Registering 'geo_point' type for %s", propertyName);
            mapping.field("type", "geo_point");
        } else if (GeoShape.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'geo_shape' type for %s", propertyName);
            mapping.field("type", "geo_shape");
            if (dataType == GeoPoint.class) {
                mapping.field("points_only", "true");
            }
            mapping.field("tree", "quadtree");
            mapping.field("precision", geoShapePrecision);
            mapping.field("distance_error_pct", geoShapeErrorPct);
        } else if (Number.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'double' type for %s", propertyName);
            mapping.field("type", "double");
        } else {
            throw new VertexiumException("Unexpected value type for property \"" + propertyName + "\": " + dataType.getName());
        }
    }

    @Override
    public synchronized void truncate(Graph graph) {
        LOGGER.warn("Truncate of Elasticsearch is not possible, dropping the indices and recreating instead.");
        drop(graph);
    }

    @Override
    public void drop(Graph graph) {
        this.indexInfosLock.writeLock().lock();
        try {
            if (this.indexInfos == null) {
                loadIndexInfos(graph);
            }
            Set<String> indexInfosSet = new HashSet<>(this.indexInfos.keySet());
            for (String indexName : indexInfosSet) {
                try {
                    DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
                    getClient().admin().indices().delete(deleteRequest).actionGet();
                } catch (Exception ex) {
                    throw new VertexiumException("Could not delete index " + indexName, ex);
                }
                this.indexInfos.remove(indexName);
                initializeIndex(graph, indexName);
            }
        } finally {
            this.indexInfosLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    protected void addPropertyValueToPropertiesMap(Map<String, Object> propertiesMap, String propertyName, Object propertyValue) {
        Object existingValue = propertiesMap.get(propertyName);
        Object valueForIndex = convertValueForIndexing(propertyValue);
        if (existingValue == null) {
            propertiesMap.put(propertyName, valueForIndex);
            return;
        }

        if (existingValue instanceof List) {
            ArrayList newList = new ArrayList<>((List) existingValue);
            newList.add(valueForIndex);
            propertiesMap.put(propertyName, newList);
            return;
        }

        List list = new ArrayList();
        list.add(existingValue);
        list.add(valueForIndex);
        propertiesMap.put(propertyName, list);
    }

    protected void convertGeoShape(Map<String, Object> propertiesMap, String propertyNameWithVisibility, GeoShape geoShape) {
        geoShape.validate();

        Map<String, Object> propertyValueMap;
        if (geoShape instanceof GeoPoint) {
            propertyValueMap = convertGeoPoint((GeoPoint) geoShape);
        } else if (geoShape instanceof GeoCircle) {
            propertyValueMap = convertGeoCircle((GeoCircle) geoShape);
        } else if (geoShape instanceof GeoLine) {
            propertyValueMap = convertGeoLine((GeoLine) geoShape);
        } else if (geoShape instanceof GeoPolygon) {
            propertyValueMap = convertGeoPolygon((GeoPolygon) geoShape);
        } else if (geoShape instanceof GeoCollection) {
            propertyValueMap = convertGeoCollection((GeoCollection) geoShape);
        } else if (geoShape instanceof GeoRect) {
            propertyValueMap = convertGeoRect((GeoRect) geoShape);
        } else {
            throw new VertexiumException("Unexpected GeoShape value of type: " + geoShape.getClass().getName());
        }

        addPropertyValueToPropertiesMap(propertiesMap, propertyNameWithVisibility + GEO_PROPERTY_NAME_SUFFIX, propertyValueMap);

        if (geoShape.getDescription() != null) {
            addPropertyValueToPropertiesMap(propertiesMap, propertyNameWithVisibility, geoShape.getDescription());
        }
    }

    protected Map<String, Object> convertGeoPoint(GeoPoint geoPoint) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "point");
        propertyValueMap.put(FIELD_COORDINATES, Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()));
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoCircle(GeoCircle geoCircle) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "circle");
        List<Double> coordinates = new ArrayList<>();
        coordinates.add(geoCircle.getLongitude());
        coordinates.add(geoCircle.getLatitude());
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        propertyValueMap.put(CircleBuilder.FIELD_RADIUS, geoCircle.getRadius() + "km");
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoRect(GeoRect geoRect) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "envelope");
        List<List<Double>> coordinates = new ArrayList<>();
        coordinates.add(Arrays.asList(geoRect.getNorthWest().getLongitude(), geoRect.getNorthWest().getLatitude()));
        coordinates.add(Arrays.asList(geoRect.getSouthEast().getLongitude(), geoRect.getSouthEast().getLatitude()));
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoLine(GeoLine geoLine) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "linestring");
        List<List<Double>> coordinates = new ArrayList<>();
        geoLine.getGeoPoints().forEach(geoPoint -> coordinates.add(Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude())));
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoPolygon(GeoPolygon geoPolygon) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "polygon");
        List<List<List<Double>>> coordinates = new ArrayList<>();
        coordinates.add(geoPolygon.getOuterBoundary().stream()
                .map(geoPoint -> Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()))
                .collect(Collectors.toList()));
        geoPolygon.getHoles().forEach(holeBoundary ->
                coordinates.add(holeBoundary.stream()
                        .map(geoPoint -> Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()))
                        .collect(Collectors.toList())));
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoCollection(GeoCollection geoCollection) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "geometrycollection");

        List<Map<String, Object>> geometries = new ArrayList<>();
        geoCollection.getGeoShapes().forEach(geoShape -> {
            if (geoShape instanceof GeoPoint) {
                geometries.add(convertGeoPoint((GeoPoint) geoShape));
            } else if (geoShape instanceof GeoCircle) {
                geometries.add(convertGeoCircle((GeoCircle) geoShape));
            } else if (geoShape instanceof GeoLine) {
                geometries.add(convertGeoLine((GeoLine) geoShape));
            } else if (geoShape instanceof GeoPolygon) {
                geometries.add(convertGeoPolygon((GeoPolygon) geoShape));
            } else {
                throw new VertexiumException("Unsupported GeoShape value in GeoCollection of type: " + geoShape.getClass().getName());
            }
        });
        propertyValueMap.put(FIELD_GEOMETRIES, geometries);

        return propertyValueMap;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    @SuppressWarnings("unused")
    protected void createIndex(String indexName) throws IOException {
        CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName)
                .setSettings(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("analysis")
                        .startObject("normalizer")
                        .startObject(LOWERCASER_NORMALIZER_NAME)
                        .field("type", "custom")
                        .array("filter", "lowercase")
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("number_of_shards", getConfig().getNumberOfShards())
                        .field("number_of_replicas", getConfig().getNumberOfReplicas())
                        .field("index.mapping.total_fields.limit", getConfig().getIndexMappingTotalFieldsLimit())
                        .endObject()
                )
                .execute().actionGet();

        ClusterHealthResponse health = client.admin().cluster().prepareHealth(indexName)
                .setWaitForGreenStatus()
                .execute().actionGet();
        LOGGER.debug("Index status: %s", health.toString());
        if (health.isTimedOut()) {
            LOGGER.warn("timed out waiting for yellow/green index status, for index: %s", indexName);
        }
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

    public IndexRefreshTracker getIndexRefreshTracker() {
        return indexRefreshTracker;
    }

    public boolean isPropertyInIndex(Graph graph, String propertyName, Visibility visibility) {
        Map<String, IndexInfo> indexInfos = getIndexInfos(graph);
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName, visibility)) {
                return true;
            }
        }
        return false;
    }

    public boolean isPropertyInIndex(Graph graph, String propertyName) {
        Map<String, IndexInfo> indexInfos = getIndexInfos(graph);
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName)) {
                return true;
            }
        }
        return false;
    }
}
