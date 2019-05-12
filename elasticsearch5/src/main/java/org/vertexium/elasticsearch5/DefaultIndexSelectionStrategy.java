package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultIndexSelectionStrategy implements IndexSelectionStrategy {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(DefaultIndexSelectionStrategy.class);
    public static final String CONFIG_INDEX_NAME = "indexName";
    public static final String DEFAULT_INDEX_NAME = "vertexium";
    public static final String CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX = "extendedDataIndexNamePrefix";
    public static final String DEFAULT_EXTENDED_DATA_INDEX_NAME_PREFIX = "vertexium_extdata_";
    public static final String CONFIG_SPLIT_EDGES_AND_VERTICES = "splitEdgesAndVertices";
    public static final boolean DEFAULT_SPLIT_VERTICES_AND_EDGES = false;
    private static final long INDEX_UPDATE_MS = 5 * 60 * 1000;
    public static final String VERTICES_INDEX_SUFFIX_NAME = "-vertices";
    public static final String EDGES_INDEX_SUFFIX_NAME = "-edges";
    private final String defaultIndexName;
    private final String extendedDataIndexNamePrefix;
    private final ReadWriteLock indicesToQueryLock = new ReentrantReadWriteLock();
    private final boolean splitEdgesAndVertices;
    private String[] indicesToQueryArray;
    private long nextUpdateTime;

    public DefaultIndexSelectionStrategy(GraphConfiguration config) {
        defaultIndexName = getDefaultIndexName(config);
        extendedDataIndexNamePrefix = getExtendedDataIndexNamePrefix(config);
        splitEdgesAndVertices = getSplitEdgesAndVertices(config);
    }

    private static String getDefaultIndexName(GraphConfiguration config) {
        String defaultIndexName = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, DEFAULT_INDEX_NAME);
        LOGGER.info("Default index name: %s", defaultIndexName);
        return defaultIndexName;
    }

    private static String getExtendedDataIndexNamePrefix(GraphConfiguration config) {
        String prefix = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, DEFAULT_EXTENDED_DATA_INDEX_NAME_PREFIX);
        LOGGER.info("Extended data index name prefix: %s", prefix);
        return prefix;
    }

    private static boolean getSplitEdgesAndVertices(GraphConfiguration config) {
        boolean splitEdgesAndVertices = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_SPLIT_EDGES_AND_VERTICES, DEFAULT_SPLIT_VERTICES_AND_EDGES);
        LOGGER.info("Split edges and vertices: %s", splitEdgesAndVertices);
        return splitEdgesAndVertices;
    }

    private void invalidateIndiciesToQueryCache() {
        nextUpdateTime = 0;
    }

    @Override
    public String[] getIndicesToQuery(Elasticsearch5SearchIndex es) {
        Lock readLock = indicesToQueryLock.readLock();
        readLock.lock();
        try {
            if (indicesToQueryArray != null && new Date().getTime() <= nextUpdateTime) {
                return indicesToQueryArray;
            }
        } finally {
            readLock.unlock();
        }
        loadIndicesToQuery(es);
        return indicesToQueryArray;
    }

    private void loadIndicesToQuery(Elasticsearch5SearchIndex es) {
        Lock writeLock = indicesToQueryLock.writeLock();
        writeLock.lock();
        try {
            Set<String> newIndicesToQuery = new HashSet<>();
            if (splitEdgesAndVertices) {
                newIndicesToQuery.add(defaultIndexName + VERTICES_INDEX_SUFFIX_NAME);
                newIndicesToQuery.add(defaultIndexName + EDGES_INDEX_SUFFIX_NAME);
            } else {
                newIndicesToQuery.add(defaultIndexName);
            }
            Set<String> indexNames = es.getIndexNamesFromElasticsearch();
            for (String indexName : indexNames) {
                if (indexName.startsWith(extendedDataIndexNamePrefix)) {
                    newIndicesToQuery.add(indexName);
                }
            }

            for (String indexName : newIndicesToQuery) {
                es.ensureIndexCreatedAndInitialized(indexName);
            }

            indicesToQueryArray = newIndicesToQuery.toArray(new String[0]);
            nextUpdateTime = new Date().getTime() + INDEX_UPDATE_MS;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getIndexName(Elasticsearch5SearchIndex es, ElementLocation elementLocation) {
        if (splitEdgesAndVertices) {
            if (elementLocation.getElementType() == ElementType.VERTEX) {
                return defaultIndexName + VERTICES_INDEX_SUFFIX_NAME;
            } else if (elementLocation.getElementType() == ElementType.EDGE) {
                return defaultIndexName + EDGES_INDEX_SUFFIX_NAME;
            } else {
                throw new VertexiumException("Unhandled element type: " + elementLocation.getClass().getName());
            }
        }
        return defaultIndexName;
    }

    @Override
    public String getExtendedDataIndexName(
        Elasticsearch5SearchIndex es,
        ElementLocation elementLocation,
        String tableName,
        String rowId
    ) {
        return getExtendedDataIndexName(es, tableName);
    }

    @Override
    public String getExtendedDataIndexName(Elasticsearch5SearchIndex es, ExtendedDataRowId rowId) {
        return getExtendedDataIndexName(es, rowId.getTableName());
    }

    private String getExtendedDataIndexName(Elasticsearch5SearchIndex es, String tableName) {
        String cleanTableName = tableName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
        String extendedDataIndexName = extendedDataIndexNamePrefix + cleanTableName;
        if (!isIncluded(es, extendedDataIndexName)) {
            invalidateIndiciesToQueryCache();
        }
        return extendedDataIndexName;
    }

    @Override
    public String[] getIndexNames(Elasticsearch5SearchIndex es, PropertyDefinition propertyDefinition) {
        return getIndicesToQuery(es);
    }

    @Override
    public boolean isIncluded(Elasticsearch5SearchIndex es, String indexName) {
        for (String i : getIndicesToQuery(es)) {
            if (i.equals(indexName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String[] getManagedIndexNames(Elasticsearch5SearchIndex es) {
        return getIndicesToQuery(es);
    }

    @Override
    public String[] getIndicesToQuery(ElasticsearchSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType) {
        return getIndicesToQuery(query.getSearchIndex());
    }
}
