package org.vertexium.elasticsearch5;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Joiner;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.vertexium.*;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoShape;
import org.vertexium.type.IpV4Address;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.vertexium.elasticsearch5.Elasticsearch5Graph.EXACT_MATCH_IGNORE_ABOVE_LIMIT;
import static org.vertexium.elasticsearch5.Elasticsearch5Graph.LOWERCASER_NORMALIZER_NAME;
import static org.vertexium.elasticsearch5.PropertyNameService.FIELDNAME_DOT_REPLACEMENT;

public class IndexService {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(IndexService.class);
    private final Elasticsearch5Graph graph;
    private final Elasticsearch5GraphConfiguration config;
    private final PropertyNameService propertyNameService;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;
    private final IdStrategy idStrategy;
    private final IndexRefreshTracker indexRefreshTracker = new IndexRefreshTracker();
    private final ReadWriteLock indexInfosLock = new ReentrantReadWriteLock();
    private Map<String, IndexInfo> indexInfos;
    private boolean metadataIndexCreatedAndInitialized;
    private final Map<String, Boolean> mutationIndexCreatedAndInitialized = new HashMap<>();

    public IndexService(
        Elasticsearch5Graph graph,
        Elasticsearch5GraphConfiguration config,
        PropertyNameService propertyNameService,
        IndexSelectionStrategy indexSelectionStrategy,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        IdStrategy idStrategy
    ) {
        this.graph = graph;
        this.config = config;
        this.propertyNameService = propertyNameService;
        this.indexSelectionStrategy = indexSelectionStrategy;
        this.propertyNameVisibilitiesStore = propertyNameVisibilitiesStore;
        this.idStrategy = idStrategy;
    }

    private Map<String, IndexInfo> getIndexInfos() {
        indexInfosLock.readLock().lock();
        try {
            if (this.indexInfos != null) {
                return new HashMap<>(this.indexInfos);
            }
        } finally {
            indexInfosLock.readLock().unlock();
        }
        return loadIndexInfos();
    }

    private Map<String, IndexInfo> loadIndexInfos() {
        indexInfosLock.writeLock().lock();
        try {
            this.indexInfos = new HashMap<>();

            Set<String> indices = getIndexNamesFromElasticsearch();
            for (String indexName : indices) {
                if (!indexSelectionStrategy.isIncluded(graph, indexName)) {
                    LOGGER.debug("skipping index %s, not in indicesToQuery", indexName);
                    continue;
                }

                LOGGER.debug("loading index info for %s", indexName);
                IndexInfo indexInfo = createIndexInfo(indexName);
                loadExistingMappingIntoIndexInfo(indexInfo, indexName);
                indexInfo.setElementTypeDefined(indexInfo.isPropertyDefined(FieldNames.ELEMENT_TYPE));
                propertyNameService.addPropertyNameVisibility(indexInfo, FieldNames.ELEMENT_ID, null);
                propertyNameService.addPropertyNameVisibility(indexInfo, FieldNames.ELEMENT_TYPE, null);
                propertyNameService.addPropertyNameVisibility(indexInfo, FieldNames.VISIBILITY, null);
                propertyNameService.addPropertyNameVisibility(indexInfo, FieldNames.OUT_VERTEX_ID, null);
                propertyNameService.addPropertyNameVisibility(indexInfo, FieldNames.IN_VERTEX_ID, null);
                propertyNameService.addPropertyNameVisibility(indexInfo, FieldNames.EDGE_LABEL, null);
                propertyNameService.addPropertyNameVisibility(indexInfo, FieldNames.ADDITIONAL_VISIBILITY, null);
                indexInfos.put(indexName, indexInfo);
            }
            return new HashMap<>(this.indexInfos);
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    private void loadExistingMappingIntoIndexInfo(IndexInfo indexInfo, String indexName) {
        indexRefreshTracker.refresh(graph.getClient(), indexName);
        LOGGER.debug("loadExistingMappingIntoIndexInfo(indexName=%s)", indexName);
        GetMappingsResponse mapping = graph.getClient().admin().indices().prepareGetMappings(indexName).get();
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
                    loadExistingPropertyMappingIntoIndexInfo(indexInfo, rawPropertyName);
                }
            }
        }
    }

    protected IndexInfo createIndexInfo(String indexName) {
        return new IndexInfo(indexName);
    }

    public Set<String> getIndexNamesFromElasticsearch() {
        LOGGER.debug("getIndexNamesFromElasticsearch()");
        return graph.getClient().admin().indices().prepareStats().execute().actionGet().getIndices().keySet();
    }

    public IndexInfo ensureIndexCreatedAndInitialized(String indexName) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        IndexInfo indexInfo = indexInfos.get(indexName);
        if (indexInfo != null && indexInfo.isElementTypeDefined()) {
            return indexInfo;
        }
        return initializeIndex(indexInfo, indexName);
    }

    protected void addPropertyToIndex(
        IndexInfo indexInfo,
        String propertyName,
        Visibility propertyVisibility,
        Class dataType,
        boolean analyzed,
        boolean exact,
        boolean sortable
    ) {
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
                .startObject(idStrategy.getType())
                .startObject("properties")
                .startObject(propertyNameService.replaceFieldnameDots(propertyName));

            addTypeToMapping(mapping, propertyName, dataType, analyzed, exact, sortable);

            mapping
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "addPropertyToIndex(indexInfo=%s, propertyName=%s, propertyVisibility=%s, dataType=%s, analyzed=%b, exact=%b, sortable=%b)",
                    indexInfo,
                    propertyName,
                    propertyVisibility,
                    dataType.getName(),
                    analyzed,
                    exact,
                    sortable
                );
            }
            graph.getClient()
                .admin()
                .indices()
                .preparePutMapping(indexInfo.getIndexName())
                .setType(idStrategy.getType())
                .setSource(mapping)
                .execute()
                .actionGet();

            propertyNameService.addPropertyNameVisibility(indexInfo, propertyName, propertyVisibility);
            updateMetadata(indexInfo);
        } catch (IOException ex) {
            throw new VertexiumException(
                String.format(
                    "Could not add property to index (index: %s, propertyName: %s)",
                    indexInfo.getIndexName(),
                    propertyName
                ),
                ex
            );
        } finally {
            this.indexInfosLock.writeLock().unlock();
        }
    }

    public void addPropertyToIndex(
        IndexInfo indexInfo,
        String propertyName,
        Object propertyValueOrClass,
        Visibility propertyVisibility
    ) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(propertyName, propertyValueOrClass);
        if (propertyDefinition != null) {
            addPropertyDefinitionToIndex(indexInfo, propertyName, propertyVisibility, propertyDefinition);
        } else {
            addPropertyToIndexInnerByPropertyValueOrClass(indexInfo, propertyName, propertyValueOrClass, propertyVisibility);
            if (!(propertyValueOrClass instanceof Class)) {
                graph.ensurePropertyDefined(propertyName, propertyValueOrClass);
            }
        }

        propertyDefinition = getPropertyDefinition(propertyName + FieldNames.EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValueOrClass);
        if (propertyDefinition != null) {
            addPropertyDefinitionToIndex(indexInfo, propertyName, propertyVisibility, propertyDefinition);
        }

        if (isInstanceOf(propertyValueOrClass, GeoShape.class)) {
            propertyDefinition = getPropertyDefinition(propertyName + FieldNames.GEO_PROPERTY_NAME_SUFFIX, propertyValueOrClass);
            if (propertyDefinition != null) {
                addPropertyDefinitionToIndex(indexInfo, propertyName, propertyVisibility, propertyDefinition);
            }
        }
    }

    public IndexInfo addPropertiesToIndex(
        ElementLocation elementLocation,
        String tableName,
        String rowId,
        List<ExtendedDataMutation> extendedData
    ) {
        String indexName = indexSelectionStrategy.getExtendedDataIndexName(graph, elementLocation, tableName, rowId);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        if (extendedData != null) {
            for (ExtendedDataMutation column : extendedData) {
                addPropertyToIndex(indexInfo, column.getColumnName(), column.getValue(), column.getVisibility());
            }
        }
        return indexInfo;
    }

    public <T extends Element> String addElementTypeVisibilityPropertyToIndex(ElementMutation<T> mutation) {
        Visibility visibility = mutation.getVisibility();
        if (mutation instanceof ExistingElementMutation) {
            ExistingElementMutation existingElementMutation = (ExistingElementMutation) mutation;
            if (existingElementMutation.getNewElementVisibility() != null) {
                visibility = existingElementMutation.getNewElementVisibility();
            }
        }
        String elementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(
            FieldNames.ELEMENT_TYPE,
            visibility
        );
        String indexName = indexSelectionStrategy.getIndexName(graph, mutation);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        addPropertyToIndex(
            indexInfo,
            elementTypeVisibilityPropertyName,
            visibility,
            String.class,
            false,
            false,
            false
        );
        return elementTypeVisibilityPropertyName;
    }

    public void ensureMutationIndexCreatedAndInitialized(String mutationIndexName) {
        if (mutationIndexCreatedAndInitialized.getOrDefault(mutationIndexName, false)) {
            return;
        }
        initializeMutationIndex(mutationIndexName);
        mutationIndexCreatedAndInitialized.put(mutationIndexName, true);
    }

    private void initializeMutationIndex(String indexName) {
        indexInfosLock.writeLock().lock();
        try {
            LOGGER.debug("initializeMutationIndex(indexName=%s) - exists?", indexName);
            if (!graph.getClient().admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
                try {
                    createIndex(indexName);
                } catch (Exception e) {
                    throw new VertexiumException("Could not create index: " + indexName, e);
                }
            }

            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_source").field("enabled", true).endObject()
                    .startObject("_all").field("enabled", false).endObject()
                    .startObject("properties")
                    .startObject(FieldNames.MUTATION_TIMESTAMP).field("type", "date").field("store", "true").endObject()
                    .startObject(FieldNames.MUTATION_ELEMENT_TYPE).field("type", "keyword").field("store", "true").endObject()
                    .startObject(FieldNames.MUTATION_ELEMENT_ID).field("type", "keyword").field("store", "true").endObject()
                    .startObject(FieldNames.MUTATION_OUT_VERTEX_ID).field("type", "keyword").field("store", "true").endObject()
                    .startObject(FieldNames.MUTATION_IN_VERTEX_ID).field("type", "keyword").field("store", "true").endObject()
                    .startObject(FieldNames.MUTATION_EDGE_LABEL).field("type", "keyword").field("store", "true").endObject()
                    .startObject(FieldNames.MUTATION_TYPE).field("type", "keyword").field("store", "true").endObject()
                    .startObject(FieldNames.MUTATION_DATA).field("type", "binary").field("store", "true").endObject();
                XContentBuilder mapping = mappingBuilder.endObject()
                    .endObject();

                LOGGER.debug("initializeMutationIndex(indexName=%s) - create", indexName);
                graph.getClient().admin().indices().preparePutMapping(indexName)
                    .setType(idStrategy.getMutationType())
                    .setSource(mapping)
                    .execute()
                    .actionGet();
            } catch (Throwable e) {
                throw new VertexiumException("Could not add mappings to index: " + indexName, e);
            }
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    public void ensureMetadataIndexCreatedAndInitialized() {
        if (metadataIndexCreatedAndInitialized) {
            return;
        }
        String indexName = getMetadataIndexName();
        initializeMetadataIndex(indexName);
        metadataIndexCreatedAndInitialized = true;
    }

    private void initializeMetadataIndex(String indexName) {
        indexInfosLock.writeLock().lock();
        try {
            LOGGER.debug("initializeMetadataIndex(indexName=%s) - exists?", indexName);
            if (!graph.getClient().admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
                try {
                    createIndex(indexName);
                } catch (Exception e) {
                    throw new VertexiumException("Could not create index: " + indexName, e);
                }
            }

            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_source").field("enabled", true).endObject()
                    .startObject("_all").field("enabled", false).endObject()
                    .startObject("properties")
                    .startObject(FieldNames.GRAPH_METADATA_NAME).field("type", "keyword").field("store", "true").endObject()
                    .startObject(FieldNames.GRAPH_METADATA_VALUE).field("type", "binary").field("store", "true").endObject();
                XContentBuilder mapping = mappingBuilder.endObject()
                    .endObject();

                LOGGER.debug("initializeMetadataIndex(indexName=%s) - create", indexName);
                graph.getClient().admin().indices().preparePutMapping(indexName)
                    .setType(idStrategy.getMetadataType())
                    .setSource(mapping)
                    .execute()
                    .actionGet();
            } catch (Throwable e) {
                throw new VertexiumException("Could not add mappings to index: " + indexName, e);
            }
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    private String getMetadataIndexName() {
        return indexSelectionStrategy.getMetadataIndexName(graph);
    }

    public String[] getIndexNames() {
        return getIndexInfos().keySet().toArray(new String[0]);
    }

    private IndexInfo initializeIndex(IndexInfo indexInfo, String indexName) {
        indexInfosLock.writeLock().lock();
        try {
            if (indexInfo == null) {
                LOGGER.debug("initializeIndex(indexName=%s)", indexName);
                if (!graph.getClient().admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
                    try {
                        createIndex(indexName);
                    } catch (Exception e) {
                        throw new VertexiumException("Could not create index: " + indexName, e);
                    }
                }

                indexInfo = createIndexInfo(indexName);

                if (indexInfos == null) {
                    loadIndexInfos();
                }
                indexInfos.put(indexName, indexInfo);
            }

            ensureMappingsCreated(indexInfo);

            return indexInfo;
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, String>> getPropertiesFromTypeMapping(MappingMetaData typeMapping) {
        return (Map<String, Map<String, String>>) typeMapping.getSourceAsMap().get("properties");
    }

    private void loadExistingPropertyMappingIntoIndexInfo(IndexInfo indexInfo, String rawPropertyName) {
        ElasticsearchPropertyNameInfo p = ElasticsearchPropertyNameInfo.parse(graph, propertyNameVisibilitiesStore, rawPropertyName);
        if (p == null) {
            return;
        }
        propertyNameService.addPropertyNameVisibility(indexInfo, rawPropertyName, p.getPropertyVisibility());
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
                    mapping.startObject(FieldNames.EXACT_MATCH)
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
            mapping.field("precision", config.getGeoShapePrecision());
            mapping.field("distance_error_pct", config.getGeoShapeErrorPct());
        } else if (Number.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'double' type for %s", propertyName);
            mapping.field("type", "double");
        } else {
            throw new VertexiumException("Unexpected value type for property \"" + propertyName + "\": " + dataType.getName());
        }
    }

    private void updateMetadata(IndexInfo indexInfo) {
        try {
            indexRefreshTracker.refresh(graph.getClient(), indexInfo.getIndexName());
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(idStrategy.getType());
            LOGGER.debug("updateMetadata(indexInfo=%s) - get mappings", indexInfo);
            GetMappingsResponse existingMapping = graph.getClient()
                .admin()
                .indices()
                .prepareGetMappings(indexInfo.getIndexName())
                .execute()
                .actionGet();

            Map<String, Object> existingElementData = existingMapping.mappings()
                .get(indexInfo.getIndexName())
                .get(idStrategy.getType())
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
                mapping.field(propertyNameService.replaceFieldnameDots(propertyName), p.getPropertyVisibility());
            }
            mapping.endObject()
                .endObject()
                .endObject()
                .endObject();

            LOGGER.debug("updateMetadata(indexInfo=%s) - put mappings", indexInfo);
            graph.getClient()
                .admin()
                .indices()
                .preparePutMapping(indexInfo.getIndexName())
                .setType(idStrategy.getType())
                .setSource(mapping)
                .execute()
                .actionGet();
        } catch (IOException ex) {
            throw new VertexiumException("Could not update mapping", ex);
        }
    }

    protected PropertyDefinition getPropertyDefinition(String propertyName, Object propertyValue) {
        propertyName = propertyNameService.removeVisibilityFromPropertyNameWithTypeSuffix(propertyName);
        return graph.getPropertyDefinition(propertyName);
    }

    private void addPropertyToIndexInner(
        IndexInfo indexInfo,
        String propertyName,
        Class<?> propertyValueClass,
        Visibility propertyVisibility
    ) {
        String propertyNameWithVisibility = propertyNameService.addVisibilityToPropertyName(propertyName, propertyVisibility);

        if (indexInfo.isPropertyDefined(propertyNameWithVisibility, propertyVisibility)) {
            return;
        }

        Class dataType;
        if (isInstanceOf(propertyValueClass, String.class)) {
            dataType = String.class;
            addPropertyToIndex(indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, true, false);
        } else if (isInstanceOf(propertyValueClass, GeoShape.class)) {
            addPropertyToIndex(indexInfo, propertyNameWithVisibility + FieldNames.GEO_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyValueClass, true, false, false);
            addPropertyToIndex(indexInfo, propertyNameWithVisibility, propertyVisibility, String.class, true, true, false);
            if (isInstanceOf(propertyValueClass, GeoPoint.class)) {
                addPropertyToIndex(indexInfo, propertyNameWithVisibility + FieldNames.GEO_POINT_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyValueClass, true, true, false);
            }
        } else {
            dataType = propertyValueClass;
            addPropertyToIndex(indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, false, false);
        }
    }

    protected void addPropertyDefinitionToIndex(
        IndexInfo indexInfo,
        String propertyName,
        Visibility propertyVisibility,
        PropertyDefinition propertyDefinition
    ) {
        String propertyNameWithVisibility = propertyNameService.addVisibilityToPropertyName(propertyName, propertyVisibility);

        if (propertyDefinition.getDataType() == String.class) {
            boolean exact = propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH);
            boolean analyzed = propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT);
            boolean sortable = propertyDefinition.isSortable();
            if (analyzed || exact || sortable) {
                addPropertyToIndex(indexInfo, propertyNameWithVisibility, propertyVisibility, String.class, analyzed, exact, sortable);
            }
            return;
        }

        if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
            addPropertyToIndex(indexInfo, propertyNameWithVisibility + FieldNames.GEO_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyDefinition.getDataType(), true, false, false);
            addPropertyToIndex(indexInfo, propertyNameWithVisibility, propertyVisibility, String.class, true, true, false);
            if (propertyDefinition.getDataType() == GeoPoint.class) {
                addPropertyToIndex(indexInfo, propertyNameWithVisibility + FieldNames.GEO_POINT_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyDefinition.getDataType(), true, true, false);
            }
            return;
        }

        addPropertyToIndex(indexInfo, propertyNameWithVisibility, propertyVisibility, propertyDefinition.getDataType(), true, false, false);
    }

    @SuppressWarnings("unused")
    protected void createIndex(String indexName) throws IOException {
        LOGGER.debug("createIndex(indexName=%s)", indexName);
        CreateIndexResponse createResponse = graph.getClient().admin().indices().prepareCreate(indexName)
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
                .field("number_of_shards", config.getNumberOfShards())
                .field("number_of_replicas", config.getNumberOfReplicas())
                .field("index.mapping.total_fields.limit", config.getIndexMappingTotalFieldsLimit())
                .field("refresh_interval", config.getIndexRefreshInterval())
                .endObject()
            )
            .execute().actionGet();

        ClusterHealthResponse health = graph.getClient().admin().cluster().prepareHealth(indexName)
            .setWaitForGreenStatus()
            .execute().actionGet();
        LOGGER.debug("Index status: %s", health.toString());
        if (health.isTimedOut()) {
            LOGGER.warn("timed out waiting for yellow/green index status, for index: %s", indexName);
        }
    }

    protected void ensureMappingsCreated(IndexInfo indexInfo) {
        if (!indexInfo.isElementTypeDefined()) {
            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_source").field("enabled", true).endObject()
                    .startObject("_all").field("enabled", false).endObject()
                    .startObject("properties");
                createIndexAddFieldsToElementType(mappingBuilder);
                XContentBuilder mapping = mappingBuilder.endObject()
                    .endObject();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("ensureMappingsCreated(indexInfo=%s)", indexInfo);
                }
                graph.getClient().admin().indices().preparePutMapping(indexInfo.getIndexName())
                    .setType(idStrategy.getType())
                    .setSource(mapping)
                    .execute()
                    .actionGet();
                indexInfo.setElementTypeDefined(true);

                updateMetadata(indexInfo);
            } catch (Throwable e) {
                throw new VertexiumException("Could not add mappings to index: " + indexInfo.getIndexName(), e);
            }
        }
    }

    protected void createIndexAddFieldsToElementType(XContentBuilder builder) throws IOException {
        builder
            .startObject(FieldNames.ELEMENT_ID).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.ELEMENT_VISIBILITY).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.ELEMENT_TYPE).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.EXTENDED_DATA_TABLE_ROW_ID).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.EXTENDED_DATA_TABLE_NAME).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.VISIBILITY).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.IN_VERTEX_ID).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.OUT_VERTEX_ID).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.EDGE_LABEL).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.ADDITIONAL_VISIBILITY).field("type", "keyword").field("store", "true").endObject()
            .startObject(FieldNames.PROPERTIES_DATA).field("type", "binary").field("store", true).endObject()
            .startObject(FieldNames.HIDDEN_VISIBILITY_DATA).field("type", "binary").field("store", true).endObject()
            .startObject(FieldNames.ADDITIONAL_VISIBILITY_DATA).field("type", "binary").field("store", true).endObject()
            .startObject(FieldNames.SOFT_DELETE_DATA).field("type", "binary").field("store", true).endObject()
        ;
    }

    public void refresh(String[] indicesToQuery) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("refresh(indicesToQuery=%s)", Joiner.on(", ").join(indicesToQuery));
        }
        indexRefreshTracker.refresh(graph.getClient(), indicesToQuery);
    }

    public boolean isPropertyInIndex(String propertyName) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName)) {
                return true;
            }
        }
        return false;
    }

    public boolean isPropertyInIndex(String propertyName, Visibility visibility) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName, visibility)) {
                return true;
            }
        }
        return false;
    }

    String addElementTypeVisibilityPropertyToExtendedDataIndex(
        ElementLocation elementLocation,
        String tableName,
        String rowId
    ) {
        String elementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(
            FieldNames.ELEMENT_TYPE,
            elementLocation.getVisibility()
        );
        String indexName = indexSelectionStrategy.getExtendedDataIndexName(graph, elementLocation, tableName, rowId);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        addPropertyToIndex(
            indexInfo,
            elementTypeVisibilityPropertyName,
            elementLocation.getVisibility(),
            String.class,
            false,
            false,
            false
        );
        return elementTypeVisibilityPropertyName;
    }

    public IndexInfo addPropertiesToIndex(
        ElementLocation elementLocation,
        Iterable<Property> properties,
        Iterable<AlterPropertyVisibility> alterPropertyVisibilities,
        Iterable<ElementMutationBase.MarkHiddenData> markHiddenData
    ) {
        String indexName = indexSelectionStrategy.getIndexName(graph, elementLocation);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        for (Property property : properties) {
            addPropertyToIndex(indexInfo, property.getName(), property.getValue(), property.getVisibility());
        }

        for (ElementMutationBase.MarkHiddenData markHidden : markHiddenData) {
            Visibility visibility = markHidden.getVisibility();
            String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(FieldNames.HIDDEN_ELEMENT, visibility);
            if (!isPropertyInIndex(FieldNames.HIDDEN_ELEMENT, visibility)) {
                addPropertyToIndex(indexInfo, hiddenVisibilityPropertyName, visibility, Boolean.class, false, false, false);
            }
        }

        for (AlterPropertyVisibility alterPropertyVisibility : alterPropertyVisibilities) {
            PropertyDefinition propertyDefinition = graph.getPropertyDefinition(alterPropertyVisibility.getName());
            if (propertyDefinition == null) {
                throw new VertexiumException("Could not find previous property definition for: " + alterPropertyVisibility.getName());
            }
            addPropertyToIndex(indexInfo, alterPropertyVisibility.getName(), propertyDefinition.getDataType(), alterPropertyVisibility.getVisibility());
        }

        return indexInfo;
    }

    private void addPropertyToIndexInnerByPropertyValueOrClass(
        IndexInfo indexInfo,
        String propertyName,
        Object propertyValueOrClass,
        Visibility propertyVisibility
    ) {
        Class propertyValueClass;
        checkNotNull(propertyValueOrClass, "property value cannot be null for property: " + propertyName);
        if (propertyValueOrClass instanceof Class) {
            propertyValueClass = (Class) propertyValueOrClass;
        } else if (propertyValueOrClass instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValueOrClass;
            if (!streamingPropertyValue.isSearchIndex()) {
                return;
            }
            propertyValueClass = streamingPropertyValue.getValueType();
        } else {
            propertyValueClass = propertyValueOrClass.getClass();
        }
        addPropertyToIndexInner(indexInfo, propertyName, propertyValueClass, propertyVisibility);
    }

    private boolean isInstanceOf(Object propertyValueOrClass, Class<?> clazz) {
        if (propertyValueOrClass instanceof Class) {
            return clazz.isAssignableFrom((Class<?>) propertyValueOrClass);
        }
        return clazz.isAssignableFrom(propertyValueOrClass.getClass());
    }

    public void pushChange(String indexName) {
        indexRefreshTracker.pushChange(indexName);
    }
}
