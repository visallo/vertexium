package org.vertexium.elasticsearch;

import org.vertexium.Element;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.GraphConfiguration;
import org.vertexium.PropertyDefinition;
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
    private static final long INDEX_UPDATE_MS = 5 * 60 * 1000;
    private final String defaultIndexName;
    private final String extendedDataIndexNamePrefix;
    private final ReadWriteLock indicesToQueryLock = new ReentrantReadWriteLock();
    private Set<String> indicesToQuery;
    private String[] indicesToQueryArray;
    private long nextUpdateTime;

    public DefaultIndexSelectionStrategy(GraphConfiguration config) {
        defaultIndexName = getDefaultIndexName(config);
        extendedDataIndexNamePrefix = getExtendedDataIndexNamePrefix(config);
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

    private void invalidateIndiciesToQueryCache() {
        nextUpdateTime = 0;
    }

    @Override
    public String[] getIndicesToQuery(ElasticsearchSingleDocumentSearchIndex es) {
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

    private Set<String> getIndicesToQuerySet(ElasticsearchSingleDocumentSearchIndex es) {
        Lock readLock = indicesToQueryLock.readLock();
        readLock.lock();
        try {
            if (indicesToQuery != null && new Date().getTime() <= nextUpdateTime) {
                return indicesToQuery;
            }
        } finally {
            readLock.unlock();
        }
        loadIndicesToQuery(es);
        return indicesToQuery;
    }

    private void loadIndicesToQuery(ElasticsearchSingleDocumentSearchIndex es) {
        Lock writeLock = indicesToQueryLock.writeLock();
        writeLock.lock();
        try {
            Set<String> newIndicesToQuery = new HashSet<>();
            newIndicesToQuery.add(defaultIndexName);
            Set<String> indexNames = es.getIndexNamesFromElasticsearch();
            for (String indexName : indexNames) {
                if (indexName.startsWith(extendedDataIndexNamePrefix)) {
                    newIndicesToQuery.add(indexName);
                }
            }

            indicesToQuery = newIndicesToQuery;
            indicesToQueryArray = newIndicesToQuery.toArray(new String[newIndicesToQuery.size()]);
            nextUpdateTime = new Date().getTime() + INDEX_UPDATE_MS;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getIndexName(ElasticsearchSingleDocumentSearchIndex es, Element element) {
        return defaultIndexName;
    }

    @Override
    public String getExtendedDataIndexName(ElasticsearchSingleDocumentSearchIndex es, Element element, String tableName, String rowId) {
        return getExtendedDataIndexName(es, tableName);
    }

    @Override
    public String getExtendedDataIndexName(ElasticsearchSingleDocumentSearchIndex es, ExtendedDataRowId rowId) {
        return getExtendedDataIndexName(es, rowId.getTableName());
    }

    private String getExtendedDataIndexName(ElasticsearchSingleDocumentSearchIndex es, String tableName) {
        String cleanTableName = tableName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
        String extendedDataIndexName = extendedDataIndexNamePrefix + cleanTableName;
        if (!isIncluded(es, extendedDataIndexName)) {
            invalidateIndiciesToQueryCache();
        }
        return extendedDataIndexName;
    }

    @Override
    public String[] getIndexNames(ElasticsearchSingleDocumentSearchIndex es, PropertyDefinition propertyDefinition) {
        return getIndicesToQuery(es);
    }

    @Override
    public boolean isIncluded(ElasticsearchSingleDocumentSearchIndex es, String indexName) {
        return getIndicesToQuerySet(es).contains(indexName);
    }

    @Override
    public String[] getManagedIndexNames(ElasticsearchSingleDocumentSearchIndex es) {
        return getIndicesToQuery(es);
    }

    @Override
    public String[] getIndicesToQuery(ElasticSearchSingleDocumentSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType) {
        return getIndicesToQuery(query.getSearchIndex());
    }
}
