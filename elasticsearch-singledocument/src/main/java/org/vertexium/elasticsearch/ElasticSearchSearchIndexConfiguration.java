package org.vertexium.elasticsearch;

import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch.score.NopScoringStrategy;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.util.ConfigurationUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchSearchIndexConfiguration {
    public static final String STORE_SOURCE_DATA = "storeSourceData";
    public static final boolean STORE_SOURCE_DATA_DEFAULT = false;
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
    public static final String AUTHORIZATION_FILTER_ENABLED = "authorizationFilterEnabled";
    public static final boolean AUTHORIZATION_FILTER_ENABLED_DEFAULT = true;
    public static final String SCORING_STRATEGY_CLASS_NAME = "scoringStrategy";
    public static final Class<? extends ScoringStrategy> SCORING_STRATEGY_CLASS_NAME_DEFAULT = NopScoringStrategy.class;
    public static final String NAME_SUBSTITUTION_STRATEGY_CLASS_NAME = "nameSubstitutionStrategy";
    public static final Class<? extends NameSubstitutionStrategy> NAME_SUBSTITUTION_STRATEGY_CLASS_NAME_DEFAULT = IdentityNameSubstitutionStrategy.class;
    public static final String INDEX_SELECTION_STRATEGY_CLASS_NAME = "indexSelectionStrategy";
    public static final Class<? extends IndexSelectionStrategy> INDEX_SELECTION_STRATEGY_CLASS_NAME_DEFAULT = DefaultIndexSelectionStrategy.class;
    public static final String ALL_FIELD_ENABLED = "allFieldEnabled";
    public static final String IN_PROCESS_NODE = "inProcessNode";
    public static final boolean IN_PROCESS_NODE_DEFAULT = false;
    public static final String IN_PROCESS_NODE_DATA_PATH = "inProcessNode.dataPath";
    public static final String IN_PROCESS_NODE_LOGS_PATH = "inProcessNode.logsPath";
    public static final String IN_PROCESS_NODE_WORK_PATH = "inProcessNode.workPath";
    public static final String IN_PROCESS_ADDITIONAL_CONFIG_PREFIX = GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + "inProcessNode.additionalConfig.";
    public static final String QUERY_PAGE_SIZE = "queryPageSize";
    public static final int QUERY_PAGE_SIZE_DEFAULT = 500;
    public static final String ES_CONFIG_FILE = "elasticsearch.configFile";
    public static final String ES_CONFIG_FILE_DEFAULT = null;

    private GraphConfiguration graphConfiguration;
    private IndexSelectionStrategy indexSelectionStrategy;
    private ScoringStrategy scoringStrategy;
    private NameSubstitutionStrategy nameSubstitutionStrategy;

    public ElasticSearchSearchIndexConfiguration(Graph graph, GraphConfiguration graphConfiguration) {
        this.graphConfiguration = graphConfiguration;
        this.scoringStrategy = getScoringStrategy(graph, graphConfiguration);
        this.nameSubstitutionStrategy = getNameSubstitutionStrategy(graph, graphConfiguration);
        this.indexSelectionStrategy = getIndexSelectionStrategy(graph, graphConfiguration);
    }

    public GraphConfiguration getGraphConfiguration() {
        return graphConfiguration;
    }

    public ScoringStrategy getScoringStrategy() {
        return scoringStrategy;
    }

    public NameSubstitutionStrategy getNameSubstitutionStrategy() {
        return nameSubstitutionStrategy;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public boolean isAutoFlush() {
        return graphConfiguration.getBoolean(GraphConfiguration.AUTO_FLUSH, AUTO_FLUSH_DEFAULT);
    }

    public boolean isStoreSourceData() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + STORE_SOURCE_DATA, STORE_SOURCE_DATA_DEFAULT);
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

    private static ScoringStrategy getScoringStrategy(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + SCORING_STRATEGY_CLASS_NAME, SCORING_STRATEGY_CLASS_NAME_DEFAULT.getName());
        return ConfigurationUtils.createProvider(className, graph, config);
    }

    private static NameSubstitutionStrategy getNameSubstitutionStrategy(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + NAME_SUBSTITUTION_STRATEGY_CLASS_NAME, NAME_SUBSTITUTION_STRATEGY_CLASS_NAME_DEFAULT.getName());
        NameSubstitutionStrategy strategy = ConfigurationUtils.createProvider(className, graph, config);
        strategy.setup(config.getConfig());
        return strategy;
    }

    public static IndexSelectionStrategy getIndexSelectionStrategy(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + INDEX_SELECTION_STRATEGY_CLASS_NAME, INDEX_SELECTION_STRATEGY_CLASS_NAME_DEFAULT.getName());
        IndexSelectionStrategy strategy = ConfigurationUtils.createProvider(className, graph, config);
        return strategy;
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

    public boolean isAuthorizationFilterEnabled() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + AUTHORIZATION_FILTER_ENABLED, AUTHORIZATION_FILTER_ENABLED_DEFAULT);
    }

    public boolean isAllFieldEnabled(boolean defaultAllFieldEnabled) {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ALL_FIELD_ENABLED, defaultAllFieldEnabled);
    }

    public boolean isInProcessNode() {
        return graphConfiguration.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + IN_PROCESS_NODE, IN_PROCESS_NODE_DEFAULT);
    }

    public String getInProcessNodeDataPath() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + IN_PROCESS_NODE_DATA_PATH, null);
    }

    public String getInProcessNodeLogsPath() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + IN_PROCESS_NODE_LOGS_PATH, null);
    }

    public String getInProcessNodeWorkPath() {
        return graphConfiguration.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + IN_PROCESS_NODE_WORK_PATH, null);
    }

    public int getQueryPageSize() {
        return graphConfiguration.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGE_SIZE, QUERY_PAGE_SIZE_DEFAULT);
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
}
