package org.vertexium.elasticsearch2.score;

import org.vertexium.GraphConfiguration;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

public class EdgeCountScoringStrategyConfiguration {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(EdgeCountScoringStrategyConfiguration.class);
    public static final String IN_EDGE_COUNT_FIELD_NAME = "__inEdgeCount";
    public static final String OUT_EDGE_COUNT_FIELD_NAME = "__outEdgeCount";
    public static final String CONFIG_USE_EDGE_BOOST = "useEdgeBoost";
    public static final boolean DEFAULT_USE_EDGE_BOOST = true;
    public static final String CONFIG_UPDATE_EDGE_BOOST = "updateEdgeBoost";
    public static final boolean DEFAULT_UPDATE_EDGE_BOOST = true;
    public static final String CONFIG_IN_EDGE_BOOST = "inEdgeBoost";
    public static final double DEFAULT_IN_EDGE_BOOST = 1.2;
    public static final String CONFIG_OUT_EDGE_BOOST = "outEdgeBoost";
    public static final double DEFAULT_OUT_EDGE_BOOST = 1.1;
    public static final String CONFIG_SCORE_FORMULA = "formula";
    public static final String DEFAULT_SCORE_FORMULA = "_score " +
            " * sqrt( " +
            "    1" +
            "    + (inEdgeMultiplier * doc['" + IN_EDGE_COUNT_FIELD_NAME + "'].value) " +
            "    + (outEdgeMultiplier * doc['" + OUT_EDGE_COUNT_FIELD_NAME + "'].value) " +
            "   )";

    private final boolean useEdgeBoost;
    private final boolean updateEdgeBoost;
    private final double inEdgeBoost;
    private final double outEdgeBoost;
    private final String scoreFormula;

    public EdgeCountScoringStrategyConfiguration(GraphConfiguration config) {
        useEdgeBoost = getUseEdgeBoost(config);
        updateEdgeBoost = getUpdateEdgeBoost(config);
        inEdgeBoost = getInEdgeBoost(config);
        outEdgeBoost = getOutEdgeBoost(config);
        scoreFormula = getScoreFormula(config);
    }

    public boolean isUseEdgeBoost() {
        return useEdgeBoost;
    }

    public boolean isUpdateEdgeBoost() {
        return isUseEdgeBoost() && updateEdgeBoost;
    }

    public double getInEdgeBoost() {
        return inEdgeBoost;
    }

    public double getOutEdgeBoost() {
        return outEdgeBoost;
    }

    public String getScoreFormula() {
        return scoreFormula;
    }

    private static boolean getUseEdgeBoost(GraphConfiguration config) {
        boolean useEdgeBoost = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_USE_EDGE_BOOST, DEFAULT_USE_EDGE_BOOST);
        LOGGER.info("Use edge boost: %b", useEdgeBoost);
        return useEdgeBoost;
    }

    private static boolean getUpdateEdgeBoost(GraphConfiguration config) {
        boolean updateEdgeBoost = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_UPDATE_EDGE_BOOST, DEFAULT_UPDATE_EDGE_BOOST);
        LOGGER.info("Update edge boost: %b", updateEdgeBoost);
        return updateEdgeBoost;
    }

    private static double getOutEdgeBoost(GraphConfiguration config) {
        double outEdgeBoost = config.getDouble(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_OUT_EDGE_BOOST, DEFAULT_OUT_EDGE_BOOST);
        LOGGER.info("Out Edge Boost: %f", outEdgeBoost);
        return outEdgeBoost;
    }

    private static double getInEdgeBoost(GraphConfiguration config) {
        double inEdgeBoost = config.getDouble(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_IN_EDGE_BOOST, DEFAULT_IN_EDGE_BOOST);
        LOGGER.info("In Edge Boost: %f", inEdgeBoost);
        return inEdgeBoost;
    }

    private static String getScoreFormula(GraphConfiguration config) {
        String scoreFormula = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_SCORE_FORMULA, DEFAULT_SCORE_FORMULA);
        LOGGER.info("Score formula: %s", scoreFormula);
        return scoreFormula;
    }
}
