package org.vertexium.elasticsearch5;

import org.elasticsearch.common.unit.TimeValue;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.util.ConfigurationUtils;

import java.io.File;
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
    public static final int NUMBER_OF_SHARDS_IN_PROCESS_DEFAULT = 1;
    public static final String NUMBER_OF_REPLICAS = "replicas";
    public static final int NUMBER_OF_REPLICAS_DEFAULT = 1;
    public static final int NUMBER_OF_REPLICAS_IN_PROCESS_DEFAULT = 0;
    public static final String TERM_AGGREGATION_SHARD_SIZE = "termAggregation.shardSize";
    public static final int TERM_AGGREGATION_SHARD_SIZE_DEFAULT = 10;
    public static final String INDEX_SELECTION_STRATEGY_CLASS_NAME = "indexSelectionStrategy";
    public static final Class<? extends IndexSelectionStrategy> INDEX_SELECTION_STRATEGY_CLASS_NAME_DEFAULT = DefaultIndexSelectionStrategy.class;
    public static final String ALL_FIELD_ENABLED = "allFieldEnabled";
    public static final String IN_PROCESS_NODE = "inProcessNode";
    public static final boolean IN_PROCESS_NODE_DEFAULT = false;
    public static final String IN_PROCESS_NODE_HOME_PATH = "inProcessNode.homePath";
    public static final String IN_PROCESS_ADDITIONAL_CONFIG_PREFIX = GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + "inProcessNode.additionalConfig.";
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
    public static final String ERROR_ON_MISSING_VERTEXIUM_PLUGIN = "errorOnMissingVertexiumPlugin";
    public static final boolean ERROR_ON_MISSING_VERTEXIUM_PLUGIN_DEFAULT = false;
    public static final String FORCE_DISABLE_VERTEXIUM_PLUGIN = "forceDisableVertexiumPlugin";
    private static final boolean FORCE_DISABLE_VERTEXIUM_PLUGIN_DEFAULT = false;
    public static final String INDEX_MAPPING_TOTAL_FIELDS_LIMIT = "indexMappingTotalFieldsLimit";
    public static final int INDEX_MAPPING_TOTAL_FIELDS_LIMIT_DEFAULT = 100000;
    public static final String PROPERTY_NAME_VISIBILITIES_STORE = "propertyNameVisibilitiesStore";
    public static final Class<? extends PropertyNameVisibilitiesStore> PROPERTY_NAME_VISIBILITIES_STORE_DEFAULT = MetadataTablePropertyNameVisibilitiesStore.class;
    public static final String GEOSHAPE_PRECISION = "geoshapePrecision";
    public static final String GEOSHAPE_PRECISION_DEFAULT = "100m";
    public static final String GEOSHAPE_ERROR_PCT = "geoshapeErrorPct";
    public static final String GEOSHAPE_ERROR_PCT_DEFAULT = "0.001";
    public static final String LOG_REQUEST_SIZE_LIMIT = "logRequestSizeLimit";
    public static final Integer LOG_REQUEST_SIZE_LIMIT_DEFAULT = null;
    public static final String MAX_QUERY_STRING_TERMS = "maxQueryStringTerms";
    public static final int MAX_QUERY_STRING_TERMS_DEFAULT = 100;

    private GraphConfiguration graphConfiguration;
    private IndexSelectionStrategy indexSelectionStrategy;

    public ElasticsearchSearchIndexConfiguration(Graph graph, GraphConfiguration graphConfiguration) {
        this.graphConfiguration = graphConfiguration;
        this.indexSelectionStrategy = getIndexSelectionStrategy(graph, graphConfiguration);
    }

    public GraphConfiguration getGraphConfiguration() {
        return graphConfiguration;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
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

    public int getNumberOfShards() {
        int numberOfShardsDefault = getNumberOfShardsDefault();
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_SHARDS, numberOfShardsDefault);
    }

    public int getNumberOfShardsDefault() {
        return isInProcessNode() ? NUMBER_OF_SHARDS_IN_PROCESS_DEFAULT : NUMBER_OF_SHARDS_DEFAULT;
    }

    public int getNumberOfReplicas() {
        int numberOfReplicasDefault = getNumberOfReplicasDefault();
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_REPLICAS, numberOfReplicasDefault);
    }

    public int getNumberOfReplicasDefault() {
        return isInProcessNode() ? NUMBER_OF_REPLICAS_IN_PROCESS_DEFAULT : NUMBER_OF_REPLICAS_DEFAULT;
    }

    public int getIndexMappingTotalFieldsLimit() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + INDEX_MAPPING_TOTAL_FIELDS_LIMIT, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_DEFAULT);
    }

    public int getTermAggregationShardSize() {
        int termAggregationShardSizeDefault = getTermAggregationShardSizeDefault();
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + TERM_AGGREGATION_SHARD_SIZE, termAggregationShardSizeDefault);
    }

    public int getTermAggregationShardSizeDefault() {
        return TERM_AGGREGATION_SHARD_SIZE_DEFAULT;
    }

    public boolean isAllFieldEnabled(boolean defaultAllFieldEnabled) {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ALL_FIELD_ENABLED, defaultAllFieldEnabled);
    }

    public boolean isInProcessNode() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + IN_PROCESS_NODE, IN_PROCESS_NODE_DEFAULT);
    }

    public boolean isErrorOnMissingVertexiumPlugin() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ERROR_ON_MISSING_VERTEXIUM_PLUGIN, ERROR_ON_MISSING_VERTEXIUM_PLUGIN_DEFAULT);
    }

    public String getInProcessNodeHomePath() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + IN_PROCESS_NODE_HOME_PATH, null);
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

    public boolean isForceDisableVertexiumPlugin() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + FORCE_DISABLE_VERTEXIUM_PLUGIN, FORCE_DISABLE_VERTEXIUM_PLUGIN_DEFAULT);
    }

    public PropertyNameVisibilitiesStore createPropertyNameVisibilitiesStore(Graph graph) {
        String className = graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + PROPERTY_NAME_VISIBILITIES_STORE, PROPERTY_NAME_VISIBILITIES_STORE_DEFAULT.getName());
        return ConfigurationUtils.createProvider(className, graph, graphConfiguration);
    }

    public File getEsConfigFile() {
        String fileName = graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ES_CONFIG_FILE, ES_CONFIG_FILE_DEFAULT);
        if (fileName == null || fileName.length() == 0) {
            return null;
        }
        return new File(fileName);
    }

    public Map<String, String> getInProcessNodeAdditionalSettings() {
        Map<String, String> results = new HashMap<>();
        for (Object o : graphConfiguration.getConfig().entrySet()) {
            Map.Entry mapEntry = (Map.Entry) o;
            if (!(mapEntry.getKey() instanceof String) || !(mapEntry.getValue() instanceof String)) {
                continue;
            }
            String key = (String) mapEntry.getKey();
            if (key.startsWith(IN_PROCESS_ADDITIONAL_CONFIG_PREFIX)) {
                String configName = key.substring(IN_PROCESS_ADDITIONAL_CONFIG_PREFIX.length());
                results.put(configName, (String) mapEntry.getValue());
            }
        }
        return results;
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
}
