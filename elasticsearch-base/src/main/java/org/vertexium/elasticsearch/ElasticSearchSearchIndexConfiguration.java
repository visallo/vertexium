package org.vertexium.elasticsearch;

import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategy;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.util.ConfigurationUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

public class ElasticSearchSearchIndexConfiguration {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticSearchSearchIndexConfiguration.class);
    public static final String CONFIG_STORE_SOURCE_DATA = "storeSourceData";
    public static final boolean DEFAULT_STORE_SOURCE_DATA = false;
    public static final String CONFIG_ES_LOCATIONS = "locations";
    public static final String CONFIG_INDEX_EDGES = "indexEdges";
    public static final boolean DEFAULT_INDEX_EDGES = true;
    public static final boolean DEFAULT_AUTO_FLUSH = false;
    public static final String CONFIG_CLUSTER_NAME = "clusterName";
    public static final String DEFAULT_CLUSTER_NAME = null;
    public static final String CONFIG_PORT = "port";
    public static final int DEFAULT_PORT = 9300;
    public static final String CONFIG_SHARDS = "shards";
    public static final int DEFAULT_NUMBER_OF_SHARDS = 5;
    public static final String CONFIG_AUTHORIZATION_FILTER_ENABLED = "authorizationFilterEnabled";
    public static final boolean DEFAULT_AUTHORIZATION_FILTER_ENABLED = true;
    public static final String CONFIG_SCORING_STRATEGY_CLASS_NAME = "scoringStrategy";
    public static final String CONFIG_NAME_SUBSTITUTION_STRATEGY_CLASS_NAME = "nameSubstitutionStrategy";
    public static final Class<? extends ScoringStrategy> DEFAULT_SCORING_STRATEGY = EdgeCountScoringStrategy.class;
    public static final Class<? extends NameSubstitutionStrategy> DEFAULT_NAME_SUBSTITUTION_STRATEGY = IdentityNameSubstitutionStrategy.class;
    public static final String CONFIG_INDEX_SELECTION_STRATEGY_CLASS_NAME = "indexSelectionStrategy";
    public static final Class<? extends IndexSelectionStrategy> DEFAULT_INDEX_SELECTION_STRATEGY = DefaultIndexSelectionStrategy.class;
    public static final String ZOOKEEPER_SERVERS = "zookeeperServers";
    public static final String DEFAULT_ZOOKEEPER_SERVERS = "localhost";

    private final boolean autoFlush;
    private final boolean storeSourceData;
    private final String[] esLocations;
    private final boolean indexEdges;
    private final String clusterName;
    private final int port;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private ScoringStrategy scoringStrategy;
    private NameSubstitutionStrategy nameSubstitutionStrategy;
    private final int numberOfShards;
    private boolean authorizationFilterEnabled;
    private String zookeeperServers;

    public ElasticSearchSearchIndexConfiguration(Graph graph, GraphConfiguration graphConfiguration) {
        esLocations = getElasticSearchLocations(graphConfiguration);
        indexEdges = getIndexEdges(graphConfiguration);
        storeSourceData = getStoreSourceData(graphConfiguration);
        autoFlush = getAutoFlush(graphConfiguration);
        clusterName = getClusterName(graphConfiguration);
        port = getPort(graphConfiguration);
        scoringStrategy = getScoringStrategy(graph, graphConfiguration);
        nameSubstitutionStrategy = getNameSubstitutionStrategy(graph, graphConfiguration);
        indexSelectionStrategy = getIndexSelectionStrategy(graph, graphConfiguration);
        numberOfShards = getNumberOfShardsForIndex(graphConfiguration);
        authorizationFilterEnabled = getAuthorizationFilterEnabled(graphConfiguration);
        zookeeperServers = getZookeeperServers(graphConfiguration);
    }

    public boolean isAutoFlush() {
        return autoFlush;
    }

    public boolean isStoreSourceData() {
        return storeSourceData;
    }

    public String[] getEsLocations() {
        return esLocations;
    }

    public boolean isIndexEdges() {
        return indexEdges;
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getPort() {
        return port;
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

    public boolean isAuthorizationFilterEnabled() {
        return authorizationFilterEnabled;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    private static boolean getAutoFlush(GraphConfiguration config) {
        boolean autoFlush = config.getBoolean(GraphConfiguration.AUTO_FLUSH, DEFAULT_AUTO_FLUSH);
        LOGGER.info("Auto flush: %b", autoFlush);
        return autoFlush;
    }

    private static boolean getStoreSourceData(GraphConfiguration config) {
        boolean storeSourceData = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_STORE_SOURCE_DATA, DEFAULT_STORE_SOURCE_DATA);
        LOGGER.info("Store source data: %b", storeSourceData);
        return storeSourceData;
    }

    private boolean getIndexEdges(GraphConfiguration config) {
        boolean indexEdges = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_EDGES, DEFAULT_INDEX_EDGES);
        LOGGER.info("index edges: %b", indexEdges);
        return indexEdges;
    }

    private static String[] getElasticSearchLocations(GraphConfiguration config) {
        String esLocationsString = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS, null);
        if (esLocationsString == null) {
            throw new VertexiumException(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS + " is a required configuration parameter");
        }
        LOGGER.info("Using elastic search locations: %s", esLocationsString);
        return esLocationsString.split(",");
    }

    private static String getClusterName(GraphConfiguration config) {
        String clusterName = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
        LOGGER.info("Cluster name: %s", clusterName);
        return clusterName;
    }

    private static int getPort(GraphConfiguration config) {
        int port = config.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_PORT, DEFAULT_PORT);
        LOGGER.info("Port: %d", port);
        return port;
    }

    private static ScoringStrategy getScoringStrategy(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_SCORING_STRATEGY_CLASS_NAME, DEFAULT_SCORING_STRATEGY.getName());
        return ConfigurationUtils.createProvider(className, graph, config);
    }

    private static NameSubstitutionStrategy getNameSubstitutionStrategy(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_NAME_SUBSTITUTION_STRATEGY_CLASS_NAME, DEFAULT_NAME_SUBSTITUTION_STRATEGY.getName());
        NameSubstitutionStrategy strategy = ConfigurationUtils.createProvider(className, graph, config);
        strategy.setup(config.getConfig());
        return strategy;
    }

    public static IndexSelectionStrategy getIndexSelectionStrategy(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_SELECTION_STRATEGY_CLASS_NAME, DEFAULT_INDEX_SELECTION_STRATEGY.getName());
        IndexSelectionStrategy strategy = ConfigurationUtils.createProvider(className, graph, config);
        return strategy;
    }

    private static int getNumberOfShardsForIndex(GraphConfiguration config) {
        int shards = config.getInt(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_SHARDS, DEFAULT_NUMBER_OF_SHARDS);
        LOGGER.info("Number of shards: %d", shards);
        return shards;
    }

    private static boolean getAuthorizationFilterEnabled(GraphConfiguration config) {
        boolean authorizationFilterEnabled = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_AUTHORIZATION_FILTER_ENABLED, DEFAULT_AUTHORIZATION_FILTER_ENABLED);
        LOGGER.info("Authorization filter enabled: %b", authorizationFilterEnabled);
        return authorizationFilterEnabled;
    }

    private static String getZookeeperServers(GraphConfiguration config) {
        String zookeeperServers = config.getString(ZOOKEEPER_SERVERS, DEFAULT_ZOOKEEPER_SERVERS);
        LOGGER.info("Zookeeper servers: %s", zookeeperServers);
        return zookeeperServers;
    }
}
