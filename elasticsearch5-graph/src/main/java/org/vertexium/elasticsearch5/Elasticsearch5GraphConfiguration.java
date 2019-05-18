package org.vertexium.elasticsearch5;

import org.elasticsearch.common.unit.TimeValue;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.ConfigurationUtils;

import java.io.File;
import java.util.*;

public class Elasticsearch5GraphConfiguration extends GraphConfiguration {
    public static final String ES_LOCATIONS = "locations";
    public static final String ES_CONFIG_FILE = "elasticsearch.configFile";
    public static final String ES_CONFIG_FILE_DEFAULT = null;
    public static final String CLUSTER_NAME = "clusterName";
    public static final String CLUSTER_NAME_DEFAULT = null;
    public static final String PORT = "port";
    public static final int PORT_DEFAULT = 9300;
    public static final String ES_SETTINGS_CONFIG_PREFIX = "esSettings.";
    public static final String ES_TRANSPORT_CLIENT_PLUGIN_CONFIG_PREFIX = "esTransportClientPlugin.";
    public static final String NUMBER_OF_SHARDS = "shards";
    public static final int NUMBER_OF_SHARDS_DEFAULT = 5;
    public static final String NUMBER_OF_REPLICAS = "replicas";
    public static final int NUMBER_OF_REPLICAS_DEFAULT = 1;
    public static final String INDEX_MAPPING_TOTAL_FIELDS_LIMIT = "indexMappingTotalFieldsLimit";
    public static final int INDEX_MAPPING_TOTAL_FIELDS_LIMIT_DEFAULT = 100000;
    public static final String INDEX_REFRESH_INTERVAL = "indexRefreshInterval";
    public static final Integer INDEX_REFRESH_INTERVAL_DEFAULT = null;
    public static final String QUERY_PAGE_SIZE = "queryPageSize";
    public static final int QUERY_PAGE_SIZE_DEFAULT = 500;
    public static final String QUERY_PAGING_LIMIT = "queryPagingLimit";
    public static final int QUERY_PAGING_LIMIT_DEFAULT = 500;
    public static final String QUERY_SCROLL_KEEP_ALIVE = "queryScrollKeepAlive";
    public static final String QUERY_SCROLL_KEEP_ALIVE_DEFAULT = "5m";
    public static final String TERM_AGGREGATION_SHARD_SIZE = "termAggregation.shardSize";
    public static final int TERM_AGGREGATION_SHARD_SIZE_DEFAULT = 10;
    public static final String MAX_QUERY_STRING_TERMS = "maxQueryStringTerms";
    public static final int MAX_QUERY_STRING_TERMS_DEFAULT = 100;
    public static final String GEOSHAPE_PRECISION = "geoshapePrecision";
    public static final String GEOSHAPE_PRECISION_DEFAULT = "100m";
    public static final String GEOSHAPE_ERROR_PCT = "geoshapeErrorPct";
    public static final String GEOSHAPE_ERROR_PCT_DEFAULT = "0.001";
    public static final String EXCEPTION_HANDLER = "exceptionHandler";
    public static final String EXCEPTION_HANDLER_DEFAULT = null;

    public Elasticsearch5GraphConfiguration(Map<String, Object> config) {
        super(config);
    }

    @Override
    public SearchIndex createSearchIndex(Graph graph) throws VertexiumException {
        return new Elasticsearch5GraphSearchIndex((Elasticsearch5Graph) graph);
    }

    public File getEsConfigFile() {
        String fileName = getString(ES_CONFIG_FILE, ES_CONFIG_FILE_DEFAULT);
        if (fileName == null || fileName.length() == 0) {
            return null;
        }
        return new File(fileName);
    }

    public String getClusterName() {
        return getString(CLUSTER_NAME, CLUSTER_NAME_DEFAULT);
    }

    public int getPort() {
        return getInt(PORT, PORT_DEFAULT);
    }

    public Map<String, String> getEsSettings() {
        Map<String, String> results = new HashMap<>();
        for (Object o : getConfig().entrySet()) {
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

    public String[] getEsLocations() {
        String esLocationsString = getString(ES_LOCATIONS, null);
        if (esLocationsString == null) {
            throw new VertexiumException(ES_LOCATIONS + " is a required configuration parameter");
        }
        return esLocationsString.split(",");
    }

    public Collection<String> getEsPluginClassNames() {
        List<String> results = new ArrayList<>();
        for (Object o : getConfig().entrySet()) {
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

    public int getNumberOfShards() {
        int numberOfShardsDefault = getNumberOfShardsDefault();
        return getInt(NUMBER_OF_SHARDS, numberOfShardsDefault);
    }

    public int getNumberOfShardsDefault() {
        return NUMBER_OF_SHARDS_DEFAULT;
    }

    public int getNumberOfReplicas() {
        int numberOfReplicasDefault = getNumberOfReplicasDefault();
        return getInt(NUMBER_OF_REPLICAS, numberOfReplicasDefault);
    }

    public int getNumberOfReplicasDefault() {
        return NUMBER_OF_REPLICAS_DEFAULT;
    }

    public int getIndexMappingTotalFieldsLimit() {
        return getInt(INDEX_MAPPING_TOTAL_FIELDS_LIMIT, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_DEFAULT);
    }

    public Integer getIndexRefreshInterval() {
        return getInteger(INDEX_REFRESH_INTERVAL, INDEX_REFRESH_INTERVAL_DEFAULT);
    }

    public int getQueryPageSize() {
        return getInt(QUERY_PAGE_SIZE, QUERY_PAGE_SIZE_DEFAULT);
    }

    public int getPagingLimit() {
        return getInt(QUERY_PAGING_LIMIT, QUERY_PAGING_LIMIT_DEFAULT);
    }

    public TimeValue getScrollKeepAlive() {
        String value = getString(QUERY_SCROLL_KEEP_ALIVE, QUERY_SCROLL_KEEP_ALIVE_DEFAULT);
        return TimeValue.parseTimeValue(value, null, "");
    }

    public int getTermAggregationShardSize() {
        int termAggregationShardSizeDefault = getTermAggregationShardSizeDefault();
        return getInt(TERM_AGGREGATION_SHARD_SIZE, termAggregationShardSizeDefault);
    }

    public int getTermAggregationShardSizeDefault() {
        return TERM_AGGREGATION_SHARD_SIZE_DEFAULT;
    }

    public int getMaxQueryStringTerms() {
        return getInt(MAX_QUERY_STRING_TERMS, MAX_QUERY_STRING_TERMS_DEFAULT);
    }

    public String getGeoShapePrecision() {
        return getString(GEOSHAPE_PRECISION, GEOSHAPE_PRECISION_DEFAULT);
    }

    public String getGeoShapeErrorPct() {
        return getString(GEOSHAPE_ERROR_PCT, GEOSHAPE_ERROR_PCT_DEFAULT);
    }

    public Elasticsearch5GraphExceptionHandler getExceptionHandler(Graph graph) {
        String className = getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + EXCEPTION_HANDLER, EXCEPTION_HANDLER_DEFAULT);
        if (className == null) {
            return null;
        }
        return ConfigurationUtils.createProvider(className, graph, this);
    }
}
