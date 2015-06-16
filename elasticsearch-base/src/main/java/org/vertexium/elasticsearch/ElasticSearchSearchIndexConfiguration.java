package org.vertexium.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch.score.EdgeCountScoringStrategy;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.id.SimpleNameSubstitutionStrategy;
import org.vertexium.id.SimpleSubstitutionUtils;
import org.vertexium.util.ConfigurationUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

public class ElasticSearchSearchIndexConfiguration {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticSearchSearchIndexConfiguration.class);
    public static final String CONFIG_STORE_SOURCE_DATA = "storeSourceData";
    public static final boolean DEFAULT_STORE_SOURCE_DATA = false;
    public static final String CONFIG_ES_LOCATIONS = "locations";
    public static final String CONFIG_INDEX_NAME = "indexName";
    public static final String DEFAULT_INDEX_NAME = "vertexium";
    public static final String CONFIG_INDICES_TO_QUERY = "indicesToQuery";
    public static final String CONFIG_INDEX_EDGES = "indexEdges";
    public static final boolean DEFAULT_INDEX_EDGES = true;
    public static final boolean DEFAULT_AUTO_FLUSH = false;
    public static final String CONFIG_CLUSTER_NAME = "clusterName";
    public static final String DEFAULT_CLUSTER_NAME = null;
    public static final String CONFIG_PORT = "port";
    public static final int DEFAULT_PORT = 9300;
    public static final String CONFIG_SCORING_STRATEGY_CLASS_NAME = "scoringStrategy";
    public static final String CONFIG_NAME_SUBSTITUTION_STRATEGY_CLASS_NAME = "nameSubstitutionStrategy";
    public static final Class<? extends ScoringStrategy> DEFAULT_SCORING_STRATEGY = EdgeCountScoringStrategy.class;
    public static final Class<? extends NameSubstitutionStrategy> DEFAULT_NAME_SUBSTITUTION_STRATEGY = IdentityNameSubstitutionStrategy.class;

    private final boolean autoFlush;
    private final boolean storeSourceData;
    private final String[] esLocations;
    private final String defaultIndexName;
    private final String[] indicesToQuery;
    private final boolean indexEdges;
    private final String clusterName;
    private final int port;
    private ScoringStrategy scoringStrategy;
    private NameSubstitutionStrategy nameSubstitutionStrategy;

    public ElasticSearchSearchIndexConfiguration(GraphConfiguration config) {
        esLocations = getElasticSearchLocations(config);
        defaultIndexName = getDefaultIndexName(config);
        indicesToQuery = getIndicesToQuery(config, defaultIndexName);
        indexEdges = getIndexEdges(config);
        storeSourceData = getStoreSourceData(config);
        autoFlush = getAutoFlush(config);
        clusterName = getClusterName(config);
        port = getPort(config);
        scoringStrategy = getScoringStrategy(config);
        nameSubstitutionStrategy = getNameSubstitutionStrategy(config);
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

    public String getDefaultIndexName() {
        return defaultIndexName;
    }

    public String[] getIndicesToQuery() {
        return indicesToQuery;
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

    private static String[] getIndicesToQuery(GraphConfiguration config, String defaultIndexName) {
        String[] indicesToQuery;
        String indicesToQueryString = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDICES_TO_QUERY, null);
        if (indicesToQueryString == null) {
            indicesToQuery = new String[]{defaultIndexName};
        } else {
            indicesToQuery = indicesToQueryString.split(",");
            for (int i = 0; i < indicesToQuery.length; i++) {
                indicesToQuery[i] = indicesToQuery[i].trim();
            }
        }
        if (LOGGER.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < indicesToQuery.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(indicesToQuery[i]);
            }
            LOGGER.info("Indices to query: %s", sb.toString());
        }
        return indicesToQuery;
    }

    private static String[] getElasticSearchLocations(GraphConfiguration config) {
        String esLocationsString = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS, null);
        if (esLocationsString == null) {
            throw new VertexiumException(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS + " is a required configuration parameter");
        }
        LOGGER.info("Using elastic search locations: %s", esLocationsString);
        return esLocationsString.split(",");
    }

    private static String getDefaultIndexName(GraphConfiguration config) {
        String defaultIndexName = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, DEFAULT_INDEX_NAME);
        LOGGER.info("Default index name: %s", defaultIndexName);
        return defaultIndexName;
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

    private static ScoringStrategy getScoringStrategy(GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_SCORING_STRATEGY_CLASS_NAME, DEFAULT_SCORING_STRATEGY.getName());
        return ConfigurationUtils.createProvider(className, config);
    }

    private static NameSubstitutionStrategy getNameSubstitutionStrategy(GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_NAME_SUBSTITUTION_STRATEGY_CLASS_NAME, DEFAULT_NAME_SUBSTITUTION_STRATEGY.getName());
        NameSubstitutionStrategy strategy = ConfigurationUtils.createProvider(className, config);
        strategy.setup(config.getConfig());

        return strategy;
    }
}
