package org.vertexium.elasticsearch7;

import org.elasticsearch.common.unit.TimeValue;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch7.bulk.BulkUpdateServiceConfiguration;
import org.vertexium.elasticsearch7.lucene.DefaultQueryStringTransformer;
import org.vertexium.elasticsearch7.lucene.QueryStringTransformer;
import org.vertexium.util.ConfigurationUtils;

import java.io.File;
import java.time.Duration;
import java.util.*;

public class ElasticsearchSearchIndexConfiguration {
    public static final String ES_LOCATIONS = "locations";
    public static final String INDEX_EDGES = "indexEdges";
    public static final boolean INDEX_EDGES_DEFAULT = true;
    public static final boolean AUTO_FLUSH_DEFAULT = false;
    public static final String CLUSTER_NAME = "clusterName";
    public static final String CLUSTER_NAME_DEFAULT = null;
    public static final String PORT = "port";
    public static final int PORT_DEFAULT = 9300;
    public static final String NUMBER_OF_SHARDS = "shards";
    public static final int NUMBER_OF_SHARDS_DEFAULT = 5;
    public static final String NUMBER_OF_REPLICAS = "replicas";
    public static final int NUMBER_OF_REPLICAS_DEFAULT = 1;
    public static final String XPACK_ENABLED = "xpack.enabled";
    public static final boolean XPACK_ENABLED_DEFAULT = false;
    public static final String TERM_AGGREGATION_SHARD_SIZE = "termAggregation.shardSize";
    public static final int TERM_AGGREGATION_SHARD_SIZE_DEFAULT = 10;
    public static final String INDEX_SELECTION_STRATEGY_CLASS_NAME = "indexSelectionStrategy";
    public static final Class<? extends IndexSelectionStrategy> INDEX_SELECTION_STRATEGY_CLASS_NAME_DEFAULT = DefaultIndexSelectionStrategy.class;
    public static final String QUERY_STRING_TRANSFORMER_CLASS_NAME = "queryStringParser";
    public static final Class<? extends QueryStringTransformer> QUERY_STRING_TRANSFORMER_CLASS_NAME_DEFAULT = DefaultQueryStringTransformer.class;
    public static final String ALL_FIELD_ENABLED = "allFieldEnabled";
    public static final String ES_SETTINGS_CONFIG_PREFIX = GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + "esSettings.";
    public static final String ES_TRANSPORT_CLIENT_PLUGIN_CONFIG_PREFIX = GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + "esTransportClientPlugin.";
    public static final String QUERY_PAGE_SIZE = "queryPageSize";
    public static final int QUERY_PAGE_SIZE_DEFAULT = 500;
    public static final String QUERY_PAGING_LIMIT = "queryPagingLimit";
    public static final int QUERY_PAGING_LIMIT_DEFAULT = 500;
    public static final String QUERY_SCROLL_KEEP_ALIVE = "queryScrollKeepAlive";
    public static final String QUERY_SCROLL_KEEP_ALIVE_DEFAULT = "5m";
    public static final String ES_CONFIG_FILE = "elasticsearch.configFile";
    public static final String ES_CONFIG_FILE_DEFAULT = null;
    public static final String INDEX_MAPPING_TOTAL_FIELDS_LIMIT = "indexMappingTotalFieldsLimit";
    public static final int INDEX_MAPPING_TOTAL_FIELDS_LIMIT_DEFAULT = 100000;
    public static final String INDEX_REFRESH_INTERVAL = "indexRefreshInterval";
    public static final String INDEX_REFRESH_INTERVAL_DEFAULT = null;
    public static final String PROPERTY_NAME_VISIBILITIES_STORE = "propertyNameVisibilitiesStore";
    public static final Class<? extends PropertyNameVisibilitiesStore> PROPERTY_NAME_VISIBILITIES_STORE_DEFAULT = MetadataTablePropertyNameVisibilitiesStore.class;
    public static final String EXCEPTION_HANDLER = "exceptionHandler";
    public static final String EXCEPTION_HANDLER_DEFAULT = null;
    public static final String GEOSHAPE_PRECISION = "geoshapePrecision";
    public static final String GEOSHAPE_PRECISION_DEFAULT = "100m";
    public static final String GEOSHAPE_ERROR_PCT = "geoshapeErrorPct";
    public static final String GEOSHAPE_ERROR_PCT_DEFAULT = "0.001";
    public static final String GEOCIRCLE_TO_POLYGON_SIDE_LENGTH = "geocircleToPolygonSideLengthKm";
    public static final double GEOCIRCLE_TO_POLYGON_SIDE_LENGTH_DEFAULT = 1.0;
    public static final String GEOCIRCLE_TO_POLYGON_MAX_NUM_SIDES = "geocircleToPolygonMaxNumSides";
    public static final int GEOCIRCLE_TO_POLYGON_MAX_NUM_SIDES_DEFAULT = 1000;
    public static final String LOG_REQUEST_SIZE_LIMIT = "logRequestSizeLimit";
    public static final Integer LOG_REQUEST_SIZE_LIMIT_DEFAULT = null;
    public static final String MAX_QUERY_STRING_TERMS = "maxQueryStringTerms";
    public static final int MAX_QUERY_STRING_TERMS_DEFAULT = 100;
    public static final String BULK_POOL_SIZE = "bulk.poolSize";
    public static final String BULK_BACKLOG_SIZE = "bulk.backlogSize";
    public static final String BULK_MAX_BATCH_SIZE = "bulk.maxBatchSize";
    public static final String BULK_MAX_BATCH_SIZE_IN_BYTES = "bulk.maxBatchSizeInBytes";
    public static final String BULK_BATCH_WINDOW_TIME = "bulk.batchWindowTime";
    public static final String BULK_MAX_FAIL_COUNT = "bulk.maxFailCount";
    public static final String BULK_REQUEST_TIMEOUT = "bulk.requestTimeout";
    public static final int BULK_POOL_SIZE_DEFAULT = BulkUpdateServiceConfiguration.POOL_SIZE_DEFAULT;
    public static final int BULK_BACKLOG_SIZE_DEFAULT = BulkUpdateServiceConfiguration.BACKLOG_SIZE_DEFAULT;
    public static final int BULK_MAX_BATCH_SIZE_DEFAULT = BulkUpdateServiceConfiguration.MAX_BATCH_SIZE_DEFAULT;
    public static final int BULK_MAX_BATCH_SIZE_IN_BYTES_DEFAULT = BulkUpdateServiceConfiguration.MAX_BATCH_SIZE_IN_BYTES_DEFAULT;
    public static final Duration BULK_BATCH_WINDOW_TIME_DEFAULT = BulkUpdateServiceConfiguration.BATCH_WINDOW_TIME_DEFAULT;
    public static final int BULK_MAX_FAIL_COUNT_DEFAULT = BulkUpdateServiceConfiguration.MAX_FAIL_COUNT_DEFAULT;
    public static final String BULK_REQUEST_TIMEOUT_DEFAULT = "30m";
    public static final String REFRESH_INDEX_ON_FLUSH = "refreshIndexOnFlush";
    public static final boolean REFRESH_INDEX_ON_FLUSH_DEFAULT = false;

    private GraphConfiguration graphConfiguration;
    private IndexSelectionStrategy indexSelectionStrategy;
    private QueryStringTransformer queryStringTransformer;

    public ElasticsearchSearchIndexConfiguration(Graph graph, GraphConfiguration graphConfiguration) {
        this.graphConfiguration = graphConfiguration;
        this.indexSelectionStrategy = getIndexSelectionStrategy(graph, graphConfiguration);
        this.queryStringTransformer = getQueryStringTransformer(graph, graphConfiguration);
    }

    public GraphConfiguration getGraphConfiguration() {
        return graphConfiguration;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public QueryStringTransformer getQueryStringTransformer() {
        return queryStringTransformer;
    }

    public void setQueryStringTransformer(QueryStringTransformer queryStringTransformer) {
        this.queryStringTransformer = queryStringTransformer;
    }

    public boolean isAutoFlush() {
        return graphConfiguration.getBoolean(GraphConfiguration.AUTO_FLUSH, AUTO_FLUSH_DEFAULT);
    }

    public boolean isIndexEdges() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + INDEX_EDGES, INDEX_EDGES_DEFAULT);
    }

    public String[] getEsLocations() {
        String esLocationsString = graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS, null);
        if (esLocationsString == null) {
            throw new VertexiumException(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS + " is a required configuration parameter");
        }
        return esLocationsString.split(",");
    }

    public String getClusterName() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CLUSTER_NAME, CLUSTER_NAME_DEFAULT);
    }

    public int getPort() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + PORT, PORT_DEFAULT);
    }

    public static IndexSelectionStrategy getIndexSelectionStrategy(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + INDEX_SELECTION_STRATEGY_CLASS_NAME, INDEX_SELECTION_STRATEGY_CLASS_NAME_DEFAULT.getName());
        return ConfigurationUtils.createProvider(className, graph, config);
    }

    public static QueryStringTransformer getQueryStringTransformer(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + QUERY_STRING_TRANSFORMER_CLASS_NAME, QUERY_STRING_TRANSFORMER_CLASS_NAME_DEFAULT.getName());
        return ConfigurationUtils.createProvider(className, graph, config);
    }

    public int getNumberOfShards() {
        int numberOfShardsDefault = getNumberOfShardsDefault();
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_SHARDS, numberOfShardsDefault);
    }

    public int getNumberOfShardsDefault() {
        return NUMBER_OF_SHARDS_DEFAULT;
    }

    public int getNumberOfReplicas() {
        int numberOfReplicasDefault = getNumberOfReplicasDefault();
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_REPLICAS, numberOfReplicasDefault);
    }

    public int getNumberOfReplicasDefault() {
        return NUMBER_OF_REPLICAS_DEFAULT;
    }

    public boolean getXpackEnabled() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + XPACK_ENABLED, XPACK_ENABLED_DEFAULT);
    }

    public int getIndexMappingTotalFieldsLimit() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + INDEX_MAPPING_TOTAL_FIELDS_LIMIT, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_DEFAULT);
    }

    public String getIndexRefreshInterval() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + INDEX_REFRESH_INTERVAL, INDEX_REFRESH_INTERVAL_DEFAULT);
    }

    public int getTermAggregationShardSize() {
        int termAggregationShardSizeDefault = getTermAggregationShardSizeDefault();
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + TERM_AGGREGATION_SHARD_SIZE, termAggregationShardSizeDefault);
    }

    public int getTermAggregationShardSizeDefault() {
        return TERM_AGGREGATION_SHARD_SIZE_DEFAULT;
    }

    public int getQueryPageSize() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGE_SIZE, QUERY_PAGE_SIZE_DEFAULT);
    }

    public int getPagingLimit() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGING_LIMIT, QUERY_PAGING_LIMIT_DEFAULT);
    }

    public TimeValue getScrollKeepAlive() {
        String value = graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + QUERY_SCROLL_KEEP_ALIVE, QUERY_SCROLL_KEEP_ALIVE_DEFAULT);
        return TimeValue.parseTimeValue(value, null, "");
    }

    public String getGeoShapePrecision() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + GEOSHAPE_PRECISION, GEOSHAPE_PRECISION_DEFAULT);
    }

    public String getGeoShapeErrorPct() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + GEOSHAPE_ERROR_PCT, GEOSHAPE_ERROR_PCT_DEFAULT);
    }

    public double getGeocircleToPolygonSideLength() {
        return graphConfiguration.getDouble(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + GEOCIRCLE_TO_POLYGON_SIDE_LENGTH, GEOCIRCLE_TO_POLYGON_SIDE_LENGTH_DEFAULT);
    }

    public int getGeocircleToPolygonMaxNumSides() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + GEOCIRCLE_TO_POLYGON_MAX_NUM_SIDES, GEOCIRCLE_TO_POLYGON_MAX_NUM_SIDES_DEFAULT);
    }

    public PropertyNameVisibilitiesStore createPropertyNameVisibilitiesStore(Graph graph) {
        String className = graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + PROPERTY_NAME_VISIBILITIES_STORE, PROPERTY_NAME_VISIBILITIES_STORE_DEFAULT.getName());
        return ConfigurationUtils.createProvider(className, graph, graphConfiguration);
    }

    public Elasticsearch7ExceptionHandler getExceptionHandler(Graph graph) {
        String className = graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + EXCEPTION_HANDLER, EXCEPTION_HANDLER_DEFAULT);
        if (className == null) {
            return null;
        }
        return ConfigurationUtils.createProvider(className, graph, graphConfiguration);
    }

    public File getEsConfigFile() {
        String fileName = graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ES_CONFIG_FILE, ES_CONFIG_FILE_DEFAULT);
        if (fileName == null || fileName.length() == 0) {
            return null;
        }
        return new File(fileName);
    }

    public Map<String, String> getEsSettings() {
        Map<String, String> results = new HashMap<>();
        for (Object o : graphConfiguration.getConfig().entrySet()) {
            Map.Entry mapEntry = (Map.Entry) o;
            if (!(mapEntry.getKey() instanceof String) || !(mapEntry.getValue() instanceof String)) {
                continue;
            }
            String key = (String) mapEntry.getKey();
            if (key.startsWith(ES_SETTINGS_CONFIG_PREFIX)) {
                String configName = key.substring(ES_SETTINGS_CONFIG_PREFIX.length());
                results.put(configName, (String) mapEntry.getValue());
            }
        }
        return results;
    }

    public Collection<String> getEsPluginClassNames() {
        List<String> results = new ArrayList<>();
        for (Object o : graphConfiguration.getConfig().entrySet()) {
            Map.Entry mapEntry = (Map.Entry) o;
            if (!(mapEntry.getKey() instanceof String) || !(mapEntry.getValue() instanceof String)) {
                continue;
            }
            String key = (String) mapEntry.getKey();
            if (key.startsWith(ES_TRANSPORT_CLIENT_PLUGIN_CONFIG_PREFIX)) {
                results.add((String) mapEntry.getValue());
            }
        }
        return results;
    }

    public Integer getLogRequestSizeLimit() {
        return graphConfiguration.getInteger(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + LOG_REQUEST_SIZE_LIMIT, LOG_REQUEST_SIZE_LIMIT_DEFAULT);
    }

    public int getMaxQueryStringTerms() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + MAX_QUERY_STRING_TERMS, MAX_QUERY_STRING_TERMS_DEFAULT);
    }

    public int getBulkPoolSize() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + BULK_POOL_SIZE, BULK_POOL_SIZE_DEFAULT);
    }

    public int getBulkBacklogSize() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + BULK_BACKLOG_SIZE, BULK_BACKLOG_SIZE_DEFAULT);
    }

    public int getBulkMaxBatchSize() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + BULK_MAX_BATCH_SIZE, BULK_MAX_BATCH_SIZE_DEFAULT);
    }

    public int getBulkMaxBatchSizeInBytes() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + BULK_MAX_BATCH_SIZE_IN_BYTES, BULK_MAX_BATCH_SIZE_IN_BYTES_DEFAULT);
    }

    public Duration getBulkBatchWindowTime() {
        return graphConfiguration.getDuration(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + BULK_BATCH_WINDOW_TIME, BULK_BATCH_WINDOW_TIME_DEFAULT);
    }

    public int getBulkMaxFailCount() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + BULK_MAX_FAIL_COUNT, BULK_MAX_FAIL_COUNT_DEFAULT);
    }

    public Duration getBulkRequestTimeout() {
        return graphConfiguration.getDuration(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + BULK_REQUEST_TIMEOUT, BULK_REQUEST_TIMEOUT_DEFAULT);
    }

    public boolean getRefreshIndexOnFlush() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + REFRESH_INDEX_ON_FLUSH, REFRESH_INDEX_ON_FLUSH_DEFAULT);
    }
}
