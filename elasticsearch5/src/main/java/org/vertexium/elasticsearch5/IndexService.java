package org.vertexium.elasticsearch5;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.vertexium.*;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.type.*;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.elasticsearch.common.geo.builders.ShapeBuilder.*;
import static org.vertexium.elasticsearch5.Elasticsearch5SearchIndex.*;
import static org.vertexium.util.StreamUtils.stream;

public class IndexService {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(IndexService.class);
    private final IndexRefreshTracker indexRefreshTracker = new IndexRefreshTracker();
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;
    private final Graph graph;
    private final Elasticsearch5SearchIndex searchIndex;
    private final Client client;
    private final ElasticsearchSearchIndexConfiguration config;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final IdStrategy idStrategy;
    private final PropertyNameService propertyNameService;
    private final ReadWriteLock indexInfosLock = new ReentrantReadWriteLock();
    private Map<String, IndexInfo> indexInfos;
    private int indexInfosLastSize = -1; // Used to prevent creating a index name array each time
    private String[] indexNamesAsArray;

    public IndexService(
        Graph graph,
        Elasticsearch5SearchIndex searchIndex,
        Client client,
        ElasticsearchSearchIndexConfiguration config,
        IndexSelectionStrategy indexSelectionStrategy,
        IdStrategy idStrategy,
        PropertyNameService propertyNameService,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore
    ) {
        this.graph = graph;
        this.searchIndex = searchIndex;
        this.client = client;
        this.config = config;
        this.indexSelectionStrategy = indexSelectionStrategy;
        this.idStrategy = idStrategy;
        this.propertyNameService = propertyNameService;
        this.propertyNameVisibilitiesStore = propertyNameVisibilitiesStore;
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
                if (!indexSelectionStrategy.isIncluded(searchIndex, indexName)) {
                    LOGGER.debug("skipping index %s, not in indicesToQuery", indexName);
                    continue;
                }

                LOGGER.debug("loading index info for %s", indexName);
                IndexInfo indexInfo = createIndexInfo(indexName);
                loadExistingMappingIntoIndexInfo(graph, indexInfo, indexName);
                indexInfo.setElementTypeDefined(indexInfo.isPropertyDefined(ELEMENT_TYPE_FIELD_NAME));
                propertyNameService.addPropertyNameVisibility(graph, indexInfo, ELEMENT_ID_FIELD_NAME, null);
                propertyNameService.addPropertyNameVisibility(graph, indexInfo, ELEMENT_TYPE_FIELD_NAME, null);
                propertyNameService.addPropertyNameVisibility(graph, indexInfo, VISIBILITY_FIELD_NAME, null);
                propertyNameService.addPropertyNameVisibility(graph, indexInfo, OUT_VERTEX_ID_FIELD_NAME, null);
                propertyNameService.addPropertyNameVisibility(graph, indexInfo, IN_VERTEX_ID_FIELD_NAME, null);
                propertyNameService.addPropertyNameVisibility(graph, indexInfo, EDGE_LABEL_FIELD_NAME, null);
                propertyNameService.addPropertyNameVisibility(graph, indexInfo, ADDITIONAL_VISIBILITY_FIELD_NAME, null);
                indexInfos.put(indexName, indexInfo);
            }
            return new HashMap<>(this.indexInfos);
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    private void loadExistingMappingIntoIndexInfo(Graph graph, IndexInfo indexInfo, String indexName) {
        indexRefreshTracker.refresh(client, indexName);
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
    }

    protected IndexInfo createIndexInfo(String indexName) {
        return new IndexInfo(indexName);
    }

    public Set<String> getIndexNamesFromElasticsearch() {
        return client.admin().indices().prepareStats().execute().actionGet().getIndices().keySet();
    }

    public String getIndexName(ElementLocation elementLocation) {
        return indexSelectionStrategy.getIndexName(searchIndex, elementLocation);
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
        Graph graph,
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
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("addPropertyToIndex: %s: %s", dataType.getName(), mapping.string());
            }

            client
                .admin()
                .indices()
                .preparePutMapping(indexInfo.getIndexName())
                .setType(idStrategy.getType())
                .setSource(mapping)
                .execute()
                .actionGet();

            propertyNameService.addPropertyNameVisibility(graph, indexInfo, propertyName, propertyVisibility);
            updateMetadata(graph, indexInfo);
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
        Graph graph,
        IndexInfo indexInfo,
        String propertyName,
        Object propertyValue,
        Visibility propertyVisibility
    ) {
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

    public IndexInfo addPropertiesToIndex(Graph graph, ElementLocation elementLocation, Iterable<Property> properties) {
        String indexName = getIndexName(elementLocation);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        for (Property property : properties) {
            addPropertyToIndex(graph, indexInfo, property.getName(), property.getValue(), property.getVisibility());
        }
        return indexInfo;
    }

    public String addElementTypeVisibilityPropertyToIndex(Graph graph, ElementLocation element) {
        String elementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(
            graph,
            ELEMENT_TYPE_FIELD_NAME,
            element.getVisibility()
        );
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        addPropertyToIndex(
            graph,
            indexInfo,
            elementTypeVisibilityPropertyName,
            element.getVisibility(),
            String.class,
            false,
            false,
            false
        );
        return elementTypeVisibilityPropertyName;
    }

    public boolean isPropertyInIndex(Graph graph, String propertyName, Visibility visibility) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName, visibility)) {
                return true;
            }
        }
        return false;
    }

    public boolean isPropertyInIndex(Graph graph, String propertyName) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName)) {
                return true;
            }
        }
        return false;
    }

    private IndexInfo initializeIndex(String indexName) {
        return initializeIndex(null, indexName);
    }

    private IndexInfo initializeIndex(IndexInfo indexInfo, String indexName) {
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

    private void loadExistingPropertyMappingIntoIndexInfo(Graph graph, IndexInfo indexInfo, String rawPropertyName) {
        ElasticsearchPropertyNameInfo p = ElasticsearchPropertyNameInfo.parse(graph, propertyNameVisibilitiesStore, rawPropertyName);
        if (p == null) {
            return;
        }
        propertyNameService.addPropertyNameVisibility(graph, indexInfo, rawPropertyName, p.getPropertyVisibility());
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
            mapping.field("precision", this.config.getGeoShapePrecision());
            mapping.field("distance_error_pct", this.config.getGeoShapeErrorPct());
        } else if (Number.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'double' type for %s", propertyName);
            mapping.field("type", "double");
        } else {
            throw new VertexiumException("Unexpected value type for property \"" + propertyName + "\": " + dataType.getName());
        }
    }


    private void updateMetadata(Graph graph, IndexInfo indexInfo) {
        try {
            indexRefreshTracker.refresh(client, indexInfo.getIndexName());
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(idStrategy.getType());
            GetMappingsResponse existingMapping = client
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
            client
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

    protected PropertyDefinition getPropertyDefinition(Graph graph, String propertyName) {
        propertyName = propertyNameService.removeVisibilityFromPropertyNameWithTypeSuffix(propertyName);
        return graph.getPropertyDefinition(propertyName);
    }

    private void addPropertyToIndexInner(
        Graph graph,
        IndexInfo indexInfo,
        String propertyName,
        Object propertyValue,
        Visibility propertyVisibility
    ) {
        String propertyNameWithVisibility = propertyNameService.addVisibilityToPropertyName(graph, propertyName, propertyVisibility);

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

    protected void addPropertyDefinitionToIndex(
        Graph graph,
        IndexInfo indexInfo,
        String propertyName,
        Visibility propertyVisibility,
        PropertyDefinition propertyDefinition
    ) {
        String propertyNameWithVisibility = propertyNameService.addVisibilityToPropertyName(graph, propertyName, propertyVisibility);

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
                .field("number_of_shards", config.getNumberOfShards())
                .field("number_of_replicas", config.getNumberOfReplicas())
                .field("index.mapping.total_fields.limit", config.getIndexMappingTotalFieldsLimit())
                .field("refresh_interval", config.getIndexRefreshInterval())
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

    protected void ensureMappingsCreated(IndexInfo indexInfo) {
        if (!indexInfo.isElementTypeDefined()) {
            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_source").field("enabled", true).endObject()
                    .startObject("_all").field("enabled", this.config.isAllFieldEnabled(false)).endObject()
                    .startObject("properties");
                createIndexAddFieldsToElementType(mappingBuilder);
                XContentBuilder mapping = mappingBuilder.endObject()
                    .endObject();

                client.admin().indices().preparePutMapping(indexInfo.getIndexName())
                    .setType(idStrategy.getType())
                    .setSource(mapping)
                    .execute()
                    .actionGet();
                indexInfo.setElementTypeDefined(true);

                updateMetadata(graph, indexInfo);
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
            .startObject(ADDITIONAL_VISIBILITY_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
        ;
    }

    public Map<String, Object> getPropertiesAsFields(Graph graph, Iterable<Property> properties) {
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

    private void addPropertyToFieldMap(Graph graph, Property property, Object propertyValue, Map<String, Object> propertiesMap) {
        String propertyName = propertyNameService.addVisibilityToPropertyName(graph, property);
        addValuesToFieldMap(graph, propertiesMap, propertyName, propertyValue);
    }

    private void addValuesToFieldMap(Graph graph, Map<String, Object> propertiesMap, String propertyName, Object propertyValue) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);
        if (propertyValue instanceof GeoShape) {
            convertGeoShape(propertiesMap, propertyName, (GeoShape) propertyValue);
            if (propertyValue instanceof GeoPoint) {
                GeoPoint geoPoint = (GeoPoint) propertyValue;
                Map<String, Double> coordinates = new HashMap<>();
                coordinates.put("lat", geoPoint.getLatitude());
                coordinates.put("lon", geoPoint.getLongitude());
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

    private void addStreamingPropertyValuesToFieldMap(Graph graph, List<Property> properties, Map<String, Object> propertiesMap) {
        List<StreamingPropertyValue> streamingPropertyValues = properties.stream()
            .map((property) -> {
                if (!(property.getValue() instanceof StreamingPropertyValue)) {
                    throw new VertexiumException("property with a value that is not a StreamingPropertyValue passed to addStreamingPropertyValuesToFieldMap");
                }
                return (StreamingPropertyValue) property.getValue();
            })
            .collect(Collectors.toList());
        if (streamingPropertyValues.size() > 0) {
            graph.flushGraph();
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

    @SuppressWarnings("unchecked")
    protected void addPropertyValueToPropertiesMap(Map<String, Object> propertiesMap, String propertyName, Object propertyValue) {
        Object existingValue = propertiesMap.get(propertyName);
        Object valueForIndex = convertValueForIndexing(propertyValue);
        if (existingValue == null) {
            propertiesMap.put(propertyName, valueForIndex);
            return;
        }

        if (existingValue instanceof List) {
            try {
                ((List) existingValue).add(valueForIndex);
            } catch (Exception ex) {
                LOGGER.error("could not add to existing list, this could cause performance issues", ex);
                ArrayList newList = new ArrayList<>((List) existingValue);
                newList.add(valueForIndex);
                propertiesMap.put(propertyName, newList);
            }
            return;
        }

        List list = new ArrayList();
        list.add(existingValue);
        list.add(valueForIndex);
        propertiesMap.put(propertyName, list);
    }

    public void pushChange(String indexName) {
        indexRefreshTracker.pushChange(indexName);
    }

    public void clearCache() {
        indexInfosLock.writeLock().lock();
        try {
            this.indexInfos = null;
            this.indexInfosLastSize = -1;
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    public void drop() {
        this.indexInfosLock.writeLock().lock();
        try {
            if (this.indexInfos == null) {
                loadIndexInfos();
            }
            Set<String> indexInfosSet = new HashSet<>(this.indexInfos.keySet());
            for (String indexName : indexInfosSet) {
                try {
                    DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
                    client.admin().indices().delete(deleteRequest).actionGet();
                } catch (Exception ex) {
                    throw new VertexiumException("Could not delete index " + indexName, ex);
                }
                this.indexInfos.remove(indexName);
                initializeIndex(indexName);
            }
        } finally {
            this.indexInfosLock.writeLock().unlock();
        }
    }

    public void refresh(String[] indicesToQuery) {
        indexRefreshTracker.refresh(client, indicesToQuery);
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

    protected String[] getIndexNamesAsArray(Graph graph) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        if (indexInfos.size() == indexInfosLastSize) {
            return indexNamesAsArray;
        }
        synchronized (this) {
            Set<String> keys = indexInfos.keySet();
            indexNamesAsArray = keys.toArray(new String[0]);
            indexInfosLastSize = indexInfos.size();
            return indexNamesAsArray;
        }
    }

    IndexInfo addExtendedDataColumnsToIndex(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String rowId,
        Iterable<ExtendedDataMutation> columns
    ) {
        String indexName = getExtendedDataIndexName(elementLocation, tableName, rowId);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        for (ExtendedDataMutation column : columns) {
            addPropertyToIndex(graph, indexInfo, column.getColumnName(), column.getValue(), column.getVisibility());
        }
        return indexInfo;
    }

    protected String getExtendedDataIndexName(
        ElementLocation elementLocation,
        String tableName,
        String rowId
    ) {
        return indexSelectionStrategy.getExtendedDataIndexName(searchIndex, elementLocation, tableName, rowId);
    }


    String addElementTypeVisibilityPropertyToExtendedDataIndex(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String rowId
    ) {
        String elementTypeVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(
            graph,
            ELEMENT_TYPE_FIELD_NAME,
            elementLocation.getVisibility()
        );
        String indexName = getExtendedDataIndexName(elementLocation, tableName, rowId);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        addPropertyToIndex(
            graph,
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

    Map<String, Object> getExtendedDataColumnsAsFields(Graph graph, Iterable<ExtendedDataMutation> columns) {
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

    @SuppressWarnings("unused")
    protected String[] getIndexNames(PropertyDefinition propertyDefinition) {
        return indexSelectionStrategy.getIndexNames(searchIndex, propertyDefinition);
    }

    protected String getExtendedDataIndexName(ExtendedDataRowId rowId) {
        return indexSelectionStrategy.getExtendedDataIndexName(searchIndex, rowId);
    }

    protected String[] getIndicesToQuery() {
        return indexSelectionStrategy.getIndicesToQuery(searchIndex);
    }

    private void addExtendedDataColumnToFieldMap(Graph graph, ExtendedDataMutation column, Object value, Map<String, Object> fieldsMap) {
        String propertyName = propertyNameService.addVisibilityToExtendedDataColumnName(graph, column);
        addValuesToFieldMap(graph, fieldsMap, propertyName, value);
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

    void addExistingValuesToFieldMap(
        Graph graph,
        Element element,
        String propertyName,
        Visibility propertyVisibility,
        Map<String, Object> fieldsToSet
    ) {
        Iterable<Property> properties = stream(element.getProperties(propertyName))
            .filter(p -> p.getVisibility().equals(propertyVisibility))
            .collect(Collectors.toList());
        Map<String, Object> remainingProperties = getPropertiesAsFields(graph, properties);
        for (Map.Entry<String, Object> remainingPropertyEntry : remainingProperties.entrySet()) {
            String remainingField = remainingPropertyEntry.getKey();
            Object remainingValue = remainingPropertyEntry.getValue();
            if (remainingValue instanceof List) {
                for (Object v : ((List) remainingValue)) {
                    addPropertyValueToPropertiesMap(fieldsToSet, remainingField, v);
                }
            } else {
                addPropertyValueToPropertiesMap(fieldsToSet, remainingField, remainingValue);
            }
        }
    }
}
