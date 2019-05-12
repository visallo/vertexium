package org.vertexium.elasticsearch5;

import org.vertexium.ElementLocation;
import org.vertexium.ElementType;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.models.Mutation;
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
    public static final String CONFIG_METADATA_INDEX_NAME = "metadataIndexName";
    public static final String CONFIG_MUTATION_INDEX_NAME = "mutationIndexName";
    public static final String CONFIG_SPLIT_EDGES_AND_VERTICES = "splitEdgesAndVertices";
    public static final String CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX = "extendedDataIndexNamePrefix";
    public static final String DEFAULT_EXTENDED_DATA_INDEX_NAME_PREFIX = "vertexium_extdata_";
    public static final boolean DEFAULT_SPLIT_VERTICES_AND_EDGES = false;
    public static final String VERTICES_INDEX_SUFFIX_NAME = "-vertices";
    public static final String EDGES_INDEX_SUFFIX_NAME = "-edges";
    private static final long INDEX_UPDATE_MS = 5 * 60 * 1000;
    private final String extendedDataIndexNamePrefix;
    private final String defaultIndexName;
    private final boolean splitEdgesAndVertices;
    private final ReadWriteLock indicesToQueryLock = new ReentrantReadWriteLock();
    private final String metadataIndexName;
    private final String mutationIndexName;
    private String[] indicesToQueryArray;
    private long nextUpdateTime;

    public DefaultIndexSelectionStrategy(GraphConfiguration config) {
        defaultIndexName = getDefaultIndexName(config);
        extendedDataIndexNamePrefix = getExtendedDataIndexNamePrefix(config);
        splitEdgesAndVertices = getSplitEdgesAndVertices(config);
        metadataIndexName = getMetadataIndexName(config, defaultIndexName);
        mutationIndexName = getMutationIndexName(config, defaultIndexName);
    }

    private String getMutationIndexName(GraphConfiguration config, String defaultIndexName) {
        String mutationIndexName = config.getString(CONFIG_MUTATION_INDEX_NAME, defaultIndexName);
        LOGGER.info("Mutation index name: %s", mutationIndexName);
        return mutationIndexName;
    }

    private String getMetadataIndexName(GraphConfiguration config, String defaultIndexName) {
        String metadataIndexName = config.getString(CONFIG_METADATA_INDEX_NAME, defaultIndexName);
        LOGGER.info("Metadata index name: %s", metadataIndexName);
        return metadataIndexName;
    }

    private static String getDefaultIndexName(GraphConfiguration config) {
        String defaultIndexName = config.getString(CONFIG_INDEX_NAME, DEFAULT_INDEX_NAME);
        LOGGER.info("Default index name: %s", defaultIndexName);
        return defaultIndexName;
    }

    private static boolean getSplitEdgesAndVertices(GraphConfiguration config) {
        boolean splitEdgesAndVertices = config.getBoolean(CONFIG_SPLIT_EDGES_AND_VERTICES, DEFAULT_SPLIT_VERTICES_AND_EDGES);
        LOGGER.info("Split edges and vertices: %s", splitEdgesAndVertices);
        return splitEdgesAndVertices;
    }

    private static String getExtendedDataIndexNamePrefix(GraphConfiguration config) {
        String prefix = config.getString(CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, DEFAULT_EXTENDED_DATA_INDEX_NAME_PREFIX);
        LOGGER.info("Extended data index name prefix: %s", prefix);
        return prefix;
    }

    @Override
    public String getIndexName(Elasticsearch5Graph es, ElementLocation elementLocation) {
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
    public String getMetadataIndexName(Elasticsearch5Graph graph) {
        return metadataIndexName;
    }

    @Override
    public String getMutationIndexName(Elasticsearch5Graph graph, Mutation mutation) {
        return mutationIndexName;
    }

    @Override
    public String getMutationIndexName(Elasticsearch5Graph graph, ElementLocation elementLocation) {
        return mutationIndexName;
    }

    @Override
    public String[] getMutationIndexNames(Elasticsearch5Graph graph) {
        return new String[]{mutationIndexName};
    }

    @Override
    public String[] getIndicesToQuery(
        Elasticsearch5Graph es,
        ElasticsearchSearchQueryBase query,
        EnumSet<ElasticsearchDocumentType> elementType
    ) {
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

    @Override
    public String getExtendedDataIndexName(Elasticsearch5Graph es, ElementLocation elementLocation, String tableName, String rowId) {
        return getExtendedDataIndexName(es, tableName);
    }

    private String getExtendedDataIndexName(Elasticsearch5Graph es, String tableName) {
        String cleanTableName = tableName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
        String extendedDataIndexName = extendedDataIndexNamePrefix + cleanTableName;
        if (!isIncluded(es, extendedDataIndexName)) {
            invalidateIndiciesToQueryCache();
        }
        return extendedDataIndexName;
    }

    private void invalidateIndiciesToQueryCache() {
        nextUpdateTime = 0;
    }

    @Override
    public String[] getIndicesToQuery(Elasticsearch5Graph es) {
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

    @Override
    public boolean isIncluded(Elasticsearch5Graph es, String indexName) {
        for (String i : getIndicesToQuery(es)) {
            if (i.equals(indexName)) {
                return true;
            }
        }
        return false;
    }

    private void loadIndicesToQuery(Elasticsearch5Graph es) {
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
            Set<String> indexNames = es.getIndexService().getIndexNamesFromElasticsearch();
            for (String indexName : indexNames) {
                if (indexName.startsWith(extendedDataIndexNamePrefix)) {
                    newIndicesToQuery.add(indexName);
                }
            }

            for (String indexName : newIndicesToQuery) {
                es.getIndexService().ensureIndexCreatedAndInitialized(indexName);
            }

            indicesToQueryArray = newIndicesToQuery.toArray(new String[0]);
            nextUpdateTime = new Date().getTime() + INDEX_UPDATE_MS;
        } finally {
            writeLock.unlock();
        }
    }
}
