package org.vertexium.accumulo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.CreateMode;
import org.vertexium.*;
import org.vertexium.HistoricalPropertyValue.HistoricalPropertyValueBuilder;
import org.vertexium.accumulo.iterator.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.IteratorFetchHints;
import org.vertexium.accumulo.iterator.model.PropertyColumnQualifier;
import org.vertexium.accumulo.iterator.model.PropertyMetadataColumnQualifier;
import org.vertexium.accumulo.iterator.util.ByteArrayWrapper;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.accumulo.util.RangeUtils;
import org.vertexium.accumulo.util.StreamingPropertyValueStorageStrategy;
import org.vertexium.event.*;
import org.vertexium.mutation.*;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.search.IndexHint;
import org.vertexium.util.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.vertexium.util.IterableUtils.singleOrDefault;
import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

public class AccumuloGraph extends GraphBaseWithSearchIndex implements Traceable {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraph.class);
    static final AccumuloGraphLogger GRAPH_LOGGER = new AccumuloGraphLogger(QUERY_LOGGER);
    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    private static final Object addIteratorLock = new Object();
    private static final Integer METADATA_ACCUMULO_GRAPH_VERSION = 2;
    private static final String METADATA_ACCUMULO_GRAPH_VERSION_KEY = "accumulo.graph.version";
    private static final String METADATA_SERIALIZER = "accumulo.graph.serializer";
    private static final String METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER = "accumulo.graph.streamingPropertyValueStorageStrategy";
    private static final Authorizations METADATA_AUTHORIZATIONS = new AccumuloAuthorizations();
    public static final int SINGLE_VERSION = 1;
    public static final Integer ALL_VERSIONS = null;
    private static final int ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY = 20;
    private static final String ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME = "vers";
    private static final ColumnVisibility EMPTY_COLUMN_VISIBILITY = new ColumnVisibility();
    private static final String CLASSPATH_CONTEXT_NAME = "vertexium";
    private final Connector connector;
    private final VertexiumSerializer vertexiumSerializer;
    private final CuratorFramework curatorFramework;
    private final boolean historyInSeparateTable;
    private final StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy;
    private final MultiTableBatchWriter batchWriter;
    protected final ElementMutationBuilder elementMutationBuilder;
    private final Queue<GraphEvent> graphEventQueue = new LinkedList<>();
    private Integer accumuloGraphVersion;
    private boolean foundVertexiumSerializerMetadata;
    private boolean foundStreamingPropertyValueStorageStrategyMetadata;
    private final AccumuloNameSubstitutionStrategy nameSubstitutionStrategy;
    private final String verticesTableName;
    private final String historyVerticesTableName;
    private final String edgesTableName;
    private final String historyEdgesTableName;
    private final String extendedDataTableName;
    private final String dataTableName;
    private final String metadataTableName;
    private final int numberOfQueryThreads;
    private final AccumuloGraphMetadataStore graphMetadataStore;
    private boolean distributedTraceEnabled;

    protected AccumuloGraph(AccumuloGraphConfiguration config, Connector connector) {
        super(config);
        this.connector = connector;
        this.vertexiumSerializer = config.createSerializer(this);
        this.nameSubstitutionStrategy = AccumuloNameSubstitutionStrategy.create(config.createSubstitutionStrategy(this));
        this.streamingPropertyValueStorageStrategy = config.createStreamingPropertyValueStorageStrategy(this);
        this.elementMutationBuilder = new ElementMutationBuilder(streamingPropertyValueStorageStrategy, vertexiumSerializer) {
            @Override
            protected void saveVertexMutation(Mutation m) {
                addMutations(VertexiumObjectType.VERTEX, m);
            }

            @Override
            protected void saveEdgeMutation(Mutation m) {
                addMutations(VertexiumObjectType.EDGE, m);
            }

            @Override
            protected void saveExtendedDataMutation(ElementType elementType, Mutation m) {
                addMutations(VertexiumObjectType.EXTENDED_DATA, m);
            }

            @Override
            protected AccumuloNameSubstitutionStrategy getNameSubstitutionStrategy() {
                return AccumuloGraph.this.getNameSubstitutionStrategy();
            }

            @Override
            public void saveDataMutation(Mutation dataMutation) {
                _addMutations(getDataWriter(), dataMutation);
            }

            @Override
            @SuppressWarnings("unchecked")
            protected StreamingPropertyValueRef saveStreamingPropertyValue(String rowKey, Property property, StreamingPropertyValue propertyValue) {
                StreamingPropertyValueRef streamingPropertyValueRef = super.saveStreamingPropertyValue(rowKey, property, propertyValue);
                ((MutableProperty) property).setValue(streamingPropertyValueRef.toStreamingPropertyValue(AccumuloGraph.this, property.getTimestamp()));
                return streamingPropertyValueRef;
            }
        };
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.newClient(config.getZookeeperServers(), retryPolicy);
        curatorFramework.start();
        String zkPath = config.getZookeeperMetadataSyncPath();
        this.graphMetadataStore = new AccumuloGraphMetadataStore(curatorFramework, zkPath);
        this.verticesTableName = getVerticesTableName(getConfiguration().getTableNamePrefix());
        this.edgesTableName = getEdgesTableName(getConfiguration().getTableNamePrefix());
        this.extendedDataTableName = getExtendedDataTableName(getConfiguration().getTableNamePrefix());
        this.dataTableName = getDataTableName(getConfiguration().getTableNamePrefix());
        this.metadataTableName = getMetadataTableName(getConfiguration().getTableNamePrefix());
        this.numberOfQueryThreads = getConfiguration().getNumberOfQueryThreads();
        this.historyInSeparateTable = getConfiguration().isHistoryInSeparateTable();

        if (isHistoryInSeparateTable()) {
            this.historyVerticesTableName = getHistoryVerticesTableName(getConfiguration().getTableNamePrefix());
            this.historyEdgesTableName = getHistoryEdgesTableName(getConfiguration().getTableNamePrefix());
        } else {
            this.historyVerticesTableName = null;
            this.historyEdgesTableName = null;
        }

        BatchWriterConfig writerConfig = getConfiguration().createBatchWriterConfig();
        this.batchWriter = connector.createMultiTableBatchWriter(writerConfig);
    }

    public static AccumuloGraph create(AccumuloGraphConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        Connector connector = config.createConnector();
        if (config.isHistoryInSeparateTable()) {
            ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
            ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());

            ensureTableExists(connector, getHistoryVerticesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
            ensureTableExists(connector, getHistoryEdgesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
            ensureRowDeletingIteratorIsAttached(connector, getHistoryVerticesTableName(config.getTableNamePrefix()));
            ensureRowDeletingIteratorIsAttached(connector, getHistoryEdgesTableName(config.getTableNamePrefix()));
        } else {
            ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
            ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
        }
        ensureTableExists(connector, getExtendedDataTableName(config.getTableNamePrefix()), config.getExtendedDataMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
        ensureTableExists(connector, getDataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
        ensureTableExists(connector, getMetadataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
        ensureRowDeletingIteratorIsAttached(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getDataTableName(config.getTableNamePrefix()));
        AccumuloGraph graph = new AccumuloGraph(config, connector);
        graph.setup();
        return graph;
    }

    @Override
    protected void setup() {
        super.setup();
        if (accumuloGraphVersion == null) {
            setMetadata(METADATA_ACCUMULO_GRAPH_VERSION_KEY, METADATA_ACCUMULO_GRAPH_VERSION);
        } else if (!METADATA_ACCUMULO_GRAPH_VERSION.equals(accumuloGraphVersion)) {
            throw new VertexiumException("Invalid accumulo graph version. Expected " + METADATA_ACCUMULO_GRAPH_VERSION + " found " + accumuloGraphVersion);
        }
    }

    @Override
    protected void setupGraphMetadata() {
        foundVertexiumSerializerMetadata = false;
        super.setupGraphMetadata();
        if (!foundVertexiumSerializerMetadata) {
            setMetadata(METADATA_SERIALIZER, vertexiumSerializer.getClass().getName());
        }
        if (!foundStreamingPropertyValueStorageStrategyMetadata) {
            setMetadata(METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER, streamingPropertyValueStorageStrategy.getClass().getName());
        }
    }

    @Override
    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        super.setupGraphMetadata(graphMetadataEntry);
        if (graphMetadataEntry.getKey().equals(METADATA_ACCUMULO_GRAPH_VERSION_KEY)) {
            if (graphMetadataEntry.getValue() instanceof Integer) {
                accumuloGraphVersion = (Integer) graphMetadataEntry.getValue();
                LOGGER.info("%s=%s", METADATA_ACCUMULO_GRAPH_VERSION_KEY, accumuloGraphVersion);
            } else {
                throw new VertexiumException("Invalid accumulo version in metadata. " + graphMetadataEntry);
            }
        } else if (graphMetadataEntry.getKey().equals(METADATA_SERIALIZER)) {
            validateClassMetadataEntry(graphMetadataEntry, vertexiumSerializer.getClass());
            foundVertexiumSerializerMetadata = true;
        } else if (graphMetadataEntry.getKey().equals(METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER)) {
            validateClassMetadataEntry(graphMetadataEntry, streamingPropertyValueStorageStrategy.getClass());
            foundStreamingPropertyValueStorageStrategyMetadata = true;
        }
    }

    private void validateClassMetadataEntry(GraphMetadataEntry graphMetadataEntry, Class expectedClass) {
        if (!(graphMetadataEntry.getValue() instanceof String)) {
            throw new VertexiumException("Invalid " + graphMetadataEntry.getKey() + " expected string found " + graphMetadataEntry.getValue().getClass().getName());
        }
        String foundClassName = (String) graphMetadataEntry.getValue();
        if (!foundClassName.equals(expectedClass.getName())) {
            throw new VertexiumException("Invalid " + graphMetadataEntry.getKey() + " expected " + foundClassName + " found " + expectedClass.getName());
        }
    }

    protected static void ensureTableExists(Connector connector, String tableName, Integer maxVersions, String hdfsContextClasspath, boolean createTable) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                if (!createTable) {
                    throw new VertexiumException("Table '" + tableName + "' does not exist and 'graph." + GraphConfiguration.CREATE_TABLES + "' is set to false");
                }
                NewTableConfiguration ntc = new NewTableConfiguration()
                        .setTimeType(TimeType.MILLIS)
                        .withoutDefaultIterators();
                connector.tableOperations().create(tableName, ntc);

                if (maxVersions != null) {
                    // The following parameters match the Accumulo defaults for the VersioningIterator
                    IteratorSetting versioningSettings = new IteratorSetting(
                            ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY,
                            ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME,
                            VersioningIterator.class
                    );
                    VersioningIterator.setMaxVersions(versioningSettings, maxVersions);
                    EnumSet<IteratorUtil.IteratorScope> scope = EnumSet.allOf(IteratorUtil.IteratorScope.class);
                    connector.tableOperations().attachIterator(tableName, versioningSettings, scope);
                }
            }

            if (hdfsContextClasspath != null) {
                connector.instanceOperations().setProperty("general.vfs.context.classpath." + CLASSPATH_CONTEXT_NAME + "-" + tableName, hdfsContextClasspath);
                connector.tableOperations().setProperty(tableName, "table.classpath.context", CLASSPATH_CONTEXT_NAME + "-" + tableName);
            }
        } catch (Exception e) {
            throw new VertexiumException("Unable to create table " + tableName, e);
        }
    }

    protected static void ensureRowDeletingIteratorIsAttached(Connector connector, String tableName) {
        try {
            synchronized (addIteratorLock) {
                IteratorSetting is = new IteratorSetting(ROW_DELETING_ITERATOR_PRIORITY, ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
                if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                    try {
                        connector.tableOperations().attachIterator(tableName, is);
                    } catch (Exception ex) {
                        // If many processes are starting up at the same time (see YARN). It's possible that there will be a collision.
                        final int SLEEP_TIME = 5000;
                        LOGGER.warn("Failed to attach RowDeletingIterator. Retrying in %dms.", SLEEP_TIME);
                        Thread.sleep(SLEEP_TIME);
                        if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                            connector.tableOperations().attachIterator(tableName, is);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new VertexiumException("Could not attach RowDeletingIterator", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static AccumuloGraph create(Map config) {
        return create(new AccumuloGraphConfiguration(config));
    }

    @Override
    public AccumuloVertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalVertexId = vertexId;
        return new AccumuloVertexBuilder(finalVertexId, visibility, elementMutationBuilder) {
            @Override
            public Vertex save(Authorizations authorizations) {
                Span trace = Trace.start("prepareVertex");
                trace.data("vertexId", finalVertexId);
                try {
                    // This has to occur before createVertex since it will mutate the properties
                    getElementMutationBuilder().saveVertexBuilder(AccumuloGraph.this, this, timestampLong);

                    AccumuloVertex vertex = createVertex(authorizations);

                    if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                        getSearchIndex().addElement(AccumuloGraph.this, vertex, authorizations);
                        getSearchIndex().addElementExtendedData(AccumuloGraph.this, vertex, getExtendedData(), authorizations);

                        for (ExtendedDataDeleteMutation m : getExtendedDataDeletes()) {
                            getSearchIndex().deleteExtendedData(
                                    AccumuloGraph.this,
                                    vertex,
                                    m.getTableName(),
                                    m.getRow(),
                                    m.getColumnName(),
                                    m.getKey(),
                                    m.getVisibility(),
                                    authorizations
                            );
                        }
                    }

                    if (hasEventListeners()) {
                        queueEvent(new AddVertexEvent(AccumuloGraph.this, vertex));
                        queueEvents(
                                vertex,
                                getProperties(),
                                getPropertyDeletes(),
                                getPropertySoftDeletes(),
                                getExtendedData(),
                                getExtendedDataDeletes()
                        );
                    }

                    return vertex;
                } finally {
                    trace.stop();
                }
            }

            @Override
            protected AccumuloVertex createVertex(Authorizations authorizations) {
                Iterable<Visibility> hiddenVisibilities = null;
                return new AccumuloVertex(
                        AccumuloGraph.this,
                        getElementId(),
                        getVisibility(),
                        getProperties(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        hiddenVisibilities,
                        getExtendedDataTableNames(),
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
            }
        };
    }

    private void queueEvents(
            AccumuloElement element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
            Iterable<ExtendedDataMutation> extendedData,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes
    ) {
        if (properties != null) {
            for (Property property : properties) {
                queueEvent(new AddPropertyEvent(this, element, property));
            }
        }
        if (propertyDeletes != null) {
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeletes) {
                queueEvent(new DeletePropertyEvent(this, element, propertyDeleteMutation));
            }
        }
        if (propertySoftDeletes != null) {
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeletes) {
                queueEvent(new SoftDeletePropertyEvent(this, element, propertySoftDeleteMutation));
            }
        }
        if (extendedData != null) {
            for (ExtendedDataMutation extendedDataMutation : extendedData) {
                queueEvent(new AddExtendedDataEvent(
                        this,
                        element,
                        extendedDataMutation.getTableName(),
                        extendedDataMutation.getRow(),
                        extendedDataMutation.getColumnName(),
                        extendedDataMutation.getKey(),
                        extendedDataMutation.getValue(),
                        extendedDataMutation.getVisibility()
                ));
            }
        }
        if (extendedDataDeletes != null) {
            for (ExtendedDataDeleteMutation extendedDataDeleteMutation : extendedDataDeletes) {
                queueEvent(new DeleteExtendedDataEvent(
                        this,
                        element,
                        extendedDataDeleteMutation.getTableName(),
                        extendedDataDeleteMutation.getRow(),
                        extendedDataDeleteMutation.getColumnName(),
                        extendedDataDeleteMutation.getKey()
                ));
            }
        }
    }

    private void queueEvent(GraphEvent graphEvent) {
        synchronized (this.graphEventQueue) {
            this.graphEventQueue.add(graphEvent);
        }
    }

    void saveProperties(
            AccumuloElement element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes
    ) {
        String elementRowKey = element.getId();
        Mutation m = new Mutation(elementRowKey);
        boolean hasProperty = false;
        for (PropertyDeleteMutation propertyDelete : propertyDeletes) {
            hasProperty = true;
            elementMutationBuilder.addPropertyDeleteToMutation(m, propertyDelete);
        }
        for (PropertySoftDeleteMutation propertySoftDelete : propertySoftDeletes) {
            hasProperty = true;
            elementMutationBuilder.addPropertySoftDeleteToMutation(m, propertySoftDelete);
        }
        for (Property property : properties) {
            hasProperty = true;
            elementMutationBuilder.addPropertyToMutation(this, m, elementRowKey, property);
        }
        if (hasProperty) {
            addMutations(element, m);
        }

        if (hasEventListeners()) {
            queueEvents(
                    element,
                    properties,
                    propertyDeletes,
                    propertySoftDeletes,
                    null,
                    null
            );
        }
    }

    void deleteProperty(AccumuloElement element, Property property, Authorizations authorizations) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new VertexiumMissingFetchHintException(element.getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }

        Mutation m = new Mutation(element.getId());
        elementMutationBuilder.addPropertyDeleteToMutation(m, property);
        addMutations(element, m);

        getSearchIndex().deleteProperty(
                this,
                element,
                PropertyDescriptor.fromProperty(property),
                authorizations
        );

        if (hasEventListeners()) {
            queueEvent(new DeletePropertyEvent(this, element, property));
        }
    }

    void softDeleteProperty(AccumuloElement element, Property property, Authorizations authorizations) {
        Mutation m = new Mutation(element.getId());
        elementMutationBuilder.addPropertySoftDeleteToMutation(m, property);
        addMutations(element, m);

        getSearchIndex().deleteProperty(
                this,
                element,
                PropertyDescriptor.fromProperty(property),
                authorizations
        );

        if (hasEventListeners()) {
            queueEvent(new SoftDeletePropertyEvent(this, element, property));
        }
    }

    protected void addMutations(Element element, Mutation... mutations) {
        addMutations(VertexiumObjectType.getTypeFromElement(element), mutations);
    }

    protected void addMutations(VertexiumObjectType objectType, Mutation... mutations) {
        _addMutations(getWriterFromElementType(objectType), mutations);
        if (isHistoryInSeparateTable() && objectType != VertexiumObjectType.EXTENDED_DATA) {
            _addMutations(getHistoryWriterFromElementType(objectType), mutations);
        }
    }

    protected void _addMutations(BatchWriter writer, Mutation... mutations) {
        try {
            for (Mutation mutation : mutations) {
                writer.addMutation(mutation);
            }
            if (getConfiguration().isAutoFlush()) {
                flush();
            }
        } catch (MutationsRejectedException ex) {
            throw new VertexiumException("Could not add mutation", ex);
        }
    }

    public BatchWriter getVerticesWriter() {
        return getWriterForTable(getVerticesTableName());
    }

    private BatchWriter getWriterForTable(String tableName) {
        try {
            return batchWriter.getBatchWriter(tableName);
        } catch (Exception e) {
            throw new VertexiumException("Unable to get writer for table " + tableName, e);
        }
    }

    public BatchWriter getHistoryVerticesWriter() {
        return getWriterForTable(getHistoryVerticesTableName());
    }

    public BatchWriter getEdgesWriter() {
        return getWriterForTable(getEdgesTableName());
    }

    public BatchWriter getHistoryEdgesWriter() {
        return getWriterForTable(getHistoryEdgesTableName());
    }

    public BatchWriter getExtendedDataWriter() {
        return getWriterForTable(getExtendedDataTableName());
    }

    public BatchWriter getDataWriter() {
        return getWriterForTable(getDataTableName());
    }

    public BatchWriter getWriterFromElementType(VertexiumObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getVerticesWriter();
            case EDGE:
                return getEdgesWriter();
            case EXTENDED_DATA:
                return getExtendedDataWriter();
            default:
                throw new VertexiumException("Unexpected object type: " + objectType);
        }
    }

    public BatchWriter getHistoryWriterFromElementType(VertexiumObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getHistoryVerticesWriter();
            case EDGE:
                return getHistoryEdgesWriter();
            default:
                throw new VertexiumException("Unexpected element type: " + objectType);
        }
    }

    protected BatchWriter getMetadataWriter() {
        return getWriterForTable(getMetadataTableName());
    }

    @Override
    public Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations) throws VertexiumException {
        Span trace = Trace.start("getVertices");
        return getVerticesInRange(trace, null, null, fetchHints, endTime, authorizations);
    }

    @Override
    public void deleteVertex(Vertex vertex, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        Span trace = Trace.start("deleteVertex");
        trace.data("vertexId", vertex.getId());
        try {
            getSearchIndex().deleteElement(this, vertex, authorizations);

            // Delete all edges that this vertex participates.
            for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
                deleteEdge(edge, authorizations);
            }

            deleteAllExtendedDataForElement(vertex, authorizations);

            addMutations(VertexiumObjectType.VERTEX, getDeleteRowMutation(vertex.getId()));

            if (hasEventListeners()) {
                queueEvent(new DeleteVertexEvent(this, vertex));
            }
        } finally {
            trace.stop();
        }
    }

    private Mutation[] getDeleteExtendedDataMutations(ExtendedDataRowId rowId) {
        Mutation[] mutations = new Mutation[1];
        Text rowKey = KeyHelper.createExtendedDataRowKey(rowId);
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
        mutations[0] = m;
        return mutations;
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        Span trace = Trace.start("softDeleteVertex");
        trace.data("vertexId", vertex.getId());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            getSearchIndex().deleteElement(this, vertex, authorizations);

            // Delete all edges that this vertex participates.
            for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
                softDeleteEdge(edge, timestamp, authorizations);
            }

            addMutations(VertexiumObjectType.VERTEX, getSoftDeleteRowMutation(vertex.getId(), timestamp));

            if (hasEventListeners()) {
                queueEvent(new SoftDeleteVertexEvent(this, vertex));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        Span trace = Trace.start("softDeleteVertex");
        trace.data("vertexId", vertex.getId());
        try {
            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            // Delete all edges that this vertex participates.
            for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
                markEdgeHidden(edge, visibility, authorizations);
            }

            addMutations(VertexiumObjectType.VERTEX, getMarkHiddenRowMutation(vertex.getId(), columnVisibility));

            getSearchIndex().markElementHidden(this, vertex, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkHiddenVertexEvent(this, vertex));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        Span trace = Trace.start("softDeleteVertex");
        trace.data("vertexId", vertex.getId());
        try {
            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            // Delete all edges that this vertex participates.
            for (Edge edge : vertex.getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
                markEdgeVisible(edge, visibility, authorizations);
            }

            addMutations(VertexiumObjectType.VERTEX, getMarkVisibleRowMutation(vertex.getId(), columnVisibility));

            getSearchIndex().markElementVisible(this, vertex, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkVisibleVertexEvent(this, vertex));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public AccumuloEdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility) {
        checkNotNull(outVertexId, "outVertexId cannot be null");
        checkNotNull(inVertexId, "inVertexId cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new AccumuloEdgeBuilderByVertexId(finalEdgeId, outVertexId, inVertexId, label, visibility, elementMutationBuilder) {
            @Override
            public Edge save(Authorizations authorizations) {
                Span trace = Trace.start("prepareEdge");
                trace.data("edgeId", finalEdgeId);
                try {
                    // This has to occur before createEdge since it will mutate the properties
                    elementMutationBuilder.saveEdgeBuilder(AccumuloGraph.this, this, timestampLong);

                    AccumuloEdge edge = AccumuloGraph.this.createEdge(
                            AccumuloGraph.this,
                            this,
                            timestampLong,
                            FetchHints.ALL_INCLUDING_HIDDEN,
                            authorizations
                    );
                    return savePreparedEdge(this, edge, null, authorizations);
                } finally {
                    trace.stop();
                }
            }

            @Override
            protected AccumuloEdge createEdge(Authorizations authorizations) {
                return AccumuloGraph.this.createEdge(
                        AccumuloGraph.this,
                        this,
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(inVertex, "inVertex cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new EdgeBuilder(finalEdgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                Span trace = Trace.start("prepareEdge");
                trace.data("edgeId", finalEdgeId);
                try {
                    AddEdgeToVertexRunnable addEdgeToVertex = new AddEdgeToVertexRunnable() {
                        @Override
                        public void run(AccumuloEdge edge) {
                            if (getOutVertex() instanceof AccumuloVertex) {
                                ((AccumuloVertex) getOutVertex()).addOutEdge(edge);
                            }
                            if (getInVertex() instanceof AccumuloVertex) {
                                ((AccumuloVertex) getInVertex()).addInEdge(edge);
                            }
                        }
                    };

                    // This has to occur before createEdge since it will mutate the properties
                    elementMutationBuilder.saveEdgeBuilder(AccumuloGraph.this, this, timestampLong);

                    AccumuloEdge edge = createEdge(
                            AccumuloGraph.this,
                            this,
                            timestampLong,
                            FetchHints.ALL_INCLUDING_HIDDEN,
                            authorizations
                    );
                    return savePreparedEdge(this, edge, addEdgeToVertex, authorizations);
                } finally {
                    trace.stop();
                }
            }
        };
    }

    private AccumuloEdge createEdge(
            AccumuloGraph accumuloGraph,
            EdgeBuilderBase edgeBuilder,
            long timestamp,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        Iterable<Visibility> hiddenVisibilities = null;
        return new AccumuloEdge(
                accumuloGraph,
                edgeBuilder.getElementId(),
                edgeBuilder.getOutVertexId(),
                edgeBuilder.getInVertexId(),
                edgeBuilder.getLabel(),
                edgeBuilder.getNewEdgeLabel(),
                edgeBuilder.getVisibility(),
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                hiddenVisibilities,
                edgeBuilder.getExtendedDataTableNames(),
                timestamp,
                fetchHints,
                authorizations
        );
    }

    private Edge savePreparedEdge(
            EdgeBuilderBase edgeBuilder,
            AccumuloEdge edge,
            AddEdgeToVertexRunnable addEdgeToVertex,
            Authorizations authorizations
    ) {
        if (addEdgeToVertex != null) {
            addEdgeToVertex.run(edge);
        }

        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElement(AccumuloGraph.this, edge, authorizations);
            getSearchIndex().addElementExtendedData(AccumuloGraph.this, edge, edgeBuilder.getExtendedData(), authorizations);
            for (ExtendedDataDeleteMutation m : edgeBuilder.getExtendedDataDeletes()) {
                getSearchIndex().deleteExtendedData(
                        AccumuloGraph.this,
                        edge,
                        m.getTableName(),
                        m.getRow(),
                        m.getColumnName(),
                        m.getKey(),
                        m.getVisibility(),
                        authorizations
                );
            }
        }

        if (hasEventListeners()) {
            queueEvent(new AddEdgeEvent(this, edge));
            queueEvents(
                    edge,
                    edgeBuilder.getProperties(),
                    edgeBuilder.getPropertyDeletes(),
                    edgeBuilder.getPropertySoftDeletes(),
                    edgeBuilder.getExtendedData(),
                    edgeBuilder.getExtendedDataDeletes()
            );
        }

        return edge;
    }

    public AccumuloNameSubstitutionStrategy getNameSubstitutionStrategy() {
        return nameSubstitutionStrategy;
    }

    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Element element, String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        Span trace = Trace.start("getHistoricalPropertyValues");
        if (Trace.isTracing()) {
            trace.data("key", key);
            trace.data("name", name);
            trace.data("visibility", visibility.getVisibilityString());
            if (startTime != null) {
                trace.data("startTime", Long.toString(startTime));
            }
            if (endTime != null) {
                trace.data("endTime", Long.toString(endTime));
            }
        }
        try {
            ElementType elementType = ElementType.getTypeFromElement(element);

            FetchHints fetchHints = FetchHints.PROPERTIES_AND_METADATA;
            traceDataFetchHints(trace, fetchHints);
            org.apache.accumulo.core.data.Range range = RangeUtils.createRangeFromString(element.getId());
            final ScannerBase scanner = createElementScanner(
                    fetchHints,
                    elementType,
                    ALL_VERSIONS,
                    startTime,
                    endTime,
                    Lists.newArrayList(range),
                    false,
                    authorizations
            );

            try {
                Map<String, HistoricalPropertyValue> results = new HashMap<>();

                ArrayListMultimap<String, String> activeVisibilities = ArrayListMultimap.create();
                Map<String, Key> softDeleteObserved = Maps.newHashMap();

                for (Map.Entry<Key, Value> column : scanner) {
                    String cq = column.getKey().getColumnQualifier().toString();
                    String columnVisibility = column.getKey().getColumnVisibility().toString();
                    if (column.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY)) {
                        if (visibility != null && !columnVisibility.equals(visibility.getVisibilityString())) {
                            continue;
                        }
                        PropertyColumnQualifier propertyColumnQualifier = KeyHelper.createPropertyColumnQualifier(cq, getNameSubstitutionStrategy());
                        if (name != null && !propertyColumnQualifier.getPropertyName().equals(name)) {
                            continue;
                        }
                        if (key != null && !propertyColumnQualifier.getPropertyKey().equals(key)) {
                            continue;
                        }
                        String resultsKey = propertyColumnQualifier.getDiscriminator(columnVisibility, column.getKey().getTimestamp());
                        long timestamp = column.getKey().getTimestamp();
                        Object value = vertexiumSerializer.bytesToObject(column.getValue().get());
                        Metadata metadata = new Metadata();
                        Set<Visibility> hiddenVisibilities = null; // TODO should we preserve these over time
                        if (value instanceof StreamingPropertyValueRef) {
                            //noinspection unchecked
                            value = ((StreamingPropertyValueRef) value).toStreamingPropertyValue(this, timestamp);
                        }
                        String propertyKey = propertyColumnQualifier.getPropertyKey();
                        String propertyName = propertyColumnQualifier.getPropertyName();
                        Visibility propertyVisibility = accumuloVisibilityToVisibility(columnVisibility);

                        HistoricalPropertyValue hpv =
                                new HistoricalPropertyValueBuilder(propertyKey, propertyName, timestamp)
                                        .propertyVisibility(propertyVisibility)
                                        .value(value)
                                        .metadata(metadata)
                                        .hiddenVisibilities(hiddenVisibilities)
                                        .build();

                        String propIdent = propertyKey + ":" + propertyName;
                        activeVisibilities.put(propIdent, columnVisibility);

                        results.put(resultsKey, hpv);

                    } else if (column.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_SOFT_DELETE)) {
                        PropertyColumnQualifier propertyColumnQualifier = KeyHelper.createPropertyColumnQualifier(cq, getNameSubstitutionStrategy());
                        String propertyKey = propertyColumnQualifier.getPropertyKey();
                        String propertyName = propertyColumnQualifier.getPropertyName();

                        String propIdent = propertyKey + ":" + propertyName;
                        activeVisibilities.remove(propIdent, columnVisibility);
                        softDeleteObserved.put(propIdent, column.getKey());

                    } else if (column.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_METADATA)) {
                        PropertyMetadataColumnQualifier propertyMetadataColumnQualifier = KeyHelper.createPropertyMetadataColumnQualifier(cq, getNameSubstitutionStrategy());
                        String resultsKey = propertyMetadataColumnQualifier.getPropertyDiscriminator(column.getKey().getTimestamp());
                        HistoricalPropertyValue hpv = results.get(resultsKey);
                        if (hpv == null) {
                            continue;
                        }
                        Object value = vertexiumSerializer.bytesToObject(column.getValue().get());
                        Visibility metadataVisibility = accumuloVisibilityToVisibility(columnVisibility);
                        hpv.getMetadata().add(propertyMetadataColumnQualifier.getMetadataKey(), value, metadataVisibility);
                    }
                }

                for (Key entry : softDeleteObserved.values()) {
                    String cq = entry.getColumnQualifier().toString();
                    PropertyColumnQualifier propertyColumnQualifier = KeyHelper.createPropertyColumnQualifier(cq, getNameSubstitutionStrategy());
                    String propertyKey = propertyColumnQualifier.getPropertyKey();
                    String propertyName = propertyColumnQualifier.getPropertyName();
                    String propIdent = propertyKey + ":" + propertyName;

                    List<String> active = activeVisibilities.get(propIdent);
                    if (active == null || active.isEmpty()) {
                        long timestamp = entry.getTimestamp() + 1;
                        String columnVisibility = entry.getColumnVisibility().toString();
                        Visibility propertyVisibility = accumuloVisibilityToVisibility(columnVisibility);

                        HistoricalPropertyValue hpv =
                                new HistoricalPropertyValueBuilder(propertyKey, propertyName, timestamp)
                                        .propertyVisibility(propertyVisibility)
                                        .isDeleted(true)
                                        .build();

                        String resultsKey = propertyColumnQualifier.getDiscriminator(columnVisibility, timestamp);
                        results.put(resultsKey, hpv);
                    }
                }

                return new TreeSet<>(results.values());
            } finally {
                scanner.close();
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        if (streamingPropertyValues.size() == 0) {
            return Collections.emptyList();
        }
        return streamingPropertyValueStorageStrategy.getInputStreams(streamingPropertyValues);
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, Authorizations authorizations) {
        List<org.apache.accumulo.core.data.Range> ranges = extendedDataRowIdToRange(ids);
        Span trace = Trace.start("getExtendedData");
        return getExtendedDataRowsInRange(trace, ranges, FetchHints.ALL, authorizations);
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(
            ElementType elementType,
            String elementId,
            String tableName,
            Authorizations authorizations
    ) {
        try {
            Span trace = Trace.start("getExtendedData");
            trace.data("elementType", elementType.name());
            trace.data("elementId", elementId);
            trace.data("tableName", tableName);
            org.apache.accumulo.core.data.Range range = org.apache.accumulo.core.data.Range.prefix(KeyHelper.createExtendedDataRowKey(elementType, elementId, tableName, ""));
            return getExtendedDataRowsInRange(trace, Lists.newArrayList(range), FetchHints.ALL, authorizations);
        } catch (IllegalStateException ex) {
            throw new VertexiumException("Failed to get extended data: " + elementType + ":" + elementId + ":" + tableName, ex);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityVertexiumException("Could not get extended data " + elementType + ":" + elementId + ":" + tableName + " with authorizations: " + authorizations, authorizations, ex.getCause());
            }
            throw ex;
        }
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, Authorizations authorizations) {
        Range extendedDataRowKeyRange = KeyHelper.createExtendedDataRowKeyRange(elementType, elementIdRange);
        return getExtendedDataInRange(extendedDataRowKeyRange, authorizations);
    }

    public Iterable<ExtendedDataRow> getExtendedDataInRange(Range extendedDataRowKeyRange, Authorizations authorizations) {
        Span trace = Trace.start("getExtendedDataInRange");
        trace.data("rangeInclusiveStart", extendedDataRowKeyRange.getInclusiveStart());
        trace.data("rangeExclusiveStart", extendedDataRowKeyRange.getExclusiveEnd());

        org.apache.accumulo.core.data.Range range = vertexiumRangeToAccumuloRange(extendedDataRowKeyRange);
        return getExtendedDataRowsInRange(trace, Collections.singletonList(range), FetchHints.ALL, authorizations);
    }

    private List<org.apache.accumulo.core.data.Range> extendedDataRowIdToRange(Iterable<ExtendedDataRowId> ids) {
        return stream(ids)
                .map(id -> org.apache.accumulo.core.data.Range.prefix(KeyHelper.createExtendedDataRowKey(id)))
                .collect(Collectors.toList());
    }

    void saveExtendedDataMutations(
            Element element,
            ElementType elementType,
            IndexHint indexHint,
            Iterable<ExtendedDataMutation> extendedData,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
            Authorizations authorizations
    ) {
        if (extendedData == null) {
            return;
        }

        String elementId = element.getId();
        elementMutationBuilder.saveExtendedDataMarkers(elementId, elementType, extendedData);
        elementMutationBuilder.saveExtendedData(this, elementId, elementType, extendedData);
        elementMutationBuilder.saveExtendedDataDeletes(this, elementId, elementType, extendedDataDeletes);

        if (indexHint != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElementExtendedData(this, element, extendedData, authorizations);
            for (ExtendedDataDeleteMutation m : extendedDataDeletes) {
                getSearchIndex().deleteExtendedData(
                        this,
                        element,
                        m.getTableName(),
                        m.getRow(),
                        m.getColumnName(),
                        m.getKey(),
                        m.getVisibility(),
                        authorizations
                );
            }
        }

        if (hasEventListeners()) {
            for (ExtendedDataMutation extendedDataMutation : extendedData) {
                queueEvent(new AddExtendedDataEvent(
                        AccumuloGraph.this,
                        element,
                        extendedDataMutation.getTableName(),
                        extendedDataMutation.getRow(),
                        extendedDataMutation.getColumnName(),
                        extendedDataMutation.getKey(),
                        extendedDataMutation.getValue(),
                        extendedDataMutation.getVisibility()
                ));
            }
            for (ExtendedDataDeleteMutation extendedDataDeleteMutation : extendedDataDeletes) {
                queueEvent(new DeleteExtendedDataEvent(
                        AccumuloGraph.this,
                        element,
                        extendedDataDeleteMutation.getTableName(),
                        extendedDataDeleteMutation.getRow(),
                        extendedDataDeleteMutation.getColumnName(),
                        extendedDataDeleteMutation.getKey()
                ));
            }
        }
    }

    private static abstract class AddEdgeToVertexRunnable {
        public abstract void run(AccumuloEdge edge);
    }

    @Override
    public CloseableIterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Span trace = Trace.start("getEdges");
        return getEdgesInRange(trace, null, null, fetchHints, endTime, authorizations);
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        checkNotNull(edge);
        Span trace = Trace.start("deleteEdge");
        trace.data("edgeId", edge.getId());
        try {
            getSearchIndex().deleteElement(this, edge, authorizations);

            ColumnVisibility visibility = visibilityToAccumuloVisibility(edge.getVisibility());

            Mutation outMutation = new Mutation(edge.getVertexId(Direction.OUT));
            outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), visibility);

            Mutation inMutation = new Mutation(edge.getVertexId(Direction.IN));
            inMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), visibility);

            addMutations(VertexiumObjectType.VERTEX, outMutation, inMutation);

            deleteAllExtendedDataForElement(edge, authorizations);

            // Deletes everything else related to edge.
            addMutations(VertexiumObjectType.EDGE, getDeleteRowMutation(edge.getId()));

            if (hasEventListeners()) {
                queueEvent(new DeleteEdgeEvent(this, edge));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void deleteExtendedDataRow(ExtendedDataRowId rowId, Authorizations authorizations) {
        checkNotNull(rowId);
        Span trace = Trace.start("deleteExtendedDataRow");
        trace.data("rowId", rowId.toString());
        try {
            getSearchIndex().deleteExtendedData(this, rowId, authorizations);

            addMutations(VertexiumObjectType.EXTENDED_DATA, getDeleteExtendedDataMutations(rowId));

            if (hasEventListeners()) {
                queueEvent(new DeleteExtendedDataRowEvent(this, rowId));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        checkNotNull(edge);
        Span trace = Trace.start("softDeleteEdge");
        trace.data("edgeId", edge.getId());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            getSearchIndex().deleteElement(this, edge, authorizations);

            ColumnVisibility visibility = visibilityToAccumuloVisibility(edge.getVisibility());

            Mutation outMutation = new Mutation(edge.getVertexId(Direction.OUT));
            outMutation.put(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE, new Text(edge.getId()), visibility, timestamp, AccumuloElement.SOFT_DELETE_VALUE);

            Mutation inMutation = new Mutation(edge.getVertexId(Direction.IN));
            inMutation.put(AccumuloVertex.CF_IN_EDGE_SOFT_DELETE, new Text(edge.getId()), visibility, timestamp, AccumuloElement.SOFT_DELETE_VALUE);

            addMutations(VertexiumObjectType.VERTEX, outMutation, inMutation);

            // Soft deletes everything else related to edge.
            addMutations(VertexiumObjectType.EDGE, getSoftDeleteRowMutation(edge.getId(), timestamp));

            if (hasEventListeners()) {
                queueEvent(new SoftDeleteEdgeEvent(this, edge));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        checkNotNull(edge);
        Span trace = Trace.start("markEdgeHidden");
        trace.data("edgeId", edge.getId());
        try {
            Vertex out = edge.getVertex(Direction.OUT, authorizations);
            if (out == null) {
                throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
            }
            Vertex in = edge.getVertex(Direction.IN, authorizations);
            if (in == null) {
                throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            Mutation outMutation = new Mutation(out.getId());
            outMutation.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, AccumuloElement.HIDDEN_VALUE);

            Mutation inMutation = new Mutation(in.getId());
            inMutation.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, AccumuloElement.HIDDEN_VALUE);

            addMutations(VertexiumObjectType.VERTEX, outMutation, inMutation);

            // Delete everything else related to edge.
            addMutations(VertexiumObjectType.EDGE, getMarkHiddenRowMutation(edge.getId(), columnVisibility));

            if (out instanceof AccumuloVertex) {
                ((AccumuloVertex) out).removeOutEdge(edge);
            }
            if (in instanceof AccumuloVertex) {
                ((AccumuloVertex) in).removeInEdge(edge);
            }

            getSearchIndex().markElementHidden(this, edge, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkHiddenEdgeEvent(this, edge));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        checkNotNull(edge);
        Span trace = Trace.start("markEdgeVisible");
        trace.data("edgeId", edge.getId());
        try {
            Vertex out = edge.getVertex(Direction.OUT, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
            if (out == null) {
                throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
            }
            Vertex in = edge.getVertex(Direction.IN, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
            if (in == null) {
                throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            Mutation outMutation = new Mutation(out.getId());
            outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility);

            Mutation inMutation = new Mutation(in.getId());
            inMutation.putDelete(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility);

            addMutations(VertexiumObjectType.VERTEX, outMutation, inMutation);

            // Delete everything else related to edge.
            addMutations(VertexiumObjectType.EDGE, getMarkVisibleRowMutation(edge.getId(), columnVisibility));

            if (out instanceof AccumuloVertex) {
                ((AccumuloVertex) out).addOutEdge(edge);
            }
            if (in instanceof AccumuloVertex) {
                ((AccumuloVertex) in).addInEdge(edge);
            }

            getSearchIndex().markElementVisible(this, edge, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkVisibleEdgeEvent(this, edge));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }

    public void markPropertyHidden(
            AccumuloElement element,
            Property property,
            Long timestamp,
            Visibility visibility,
            @SuppressWarnings("UnusedParameters") Authorizations authorizations
    ) {
        checkNotNull(element);
        Span trace = Trace.start("markPropertyHidden");
        trace.data("elementId", element.getId());
        trace.data("propertyName", property.getName());
        trace.data("propertyKey", property.getKey());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            if (element instanceof Vertex) {
                addMutations(VertexiumObjectType.VERTEX, getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility));
            } else if (element instanceof Edge) {
                addMutations(VertexiumObjectType.EDGE, getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility));
            }

            getSearchIndex().markPropertyHidden(this, element, property, visibility, authorizations);

            if (hasEventListeners()) {
                fireGraphEvent(new MarkHiddenPropertyEvent(this, element, property, visibility));
            }
        } finally {
            trace.stop();
        }
    }

    private Mutation getMarkHiddenPropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility) {
        Mutation m = new Mutation(rowKey);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(property, getNameSubstitutionStrategy());
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, timestamp, AccumuloElement.HIDDEN_VALUE);
        return m;
    }

    @SuppressWarnings("unused")
    public void markPropertyVisible(AccumuloElement element, Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element);
        Span trace = Trace.start("markPropertyVisible");
        trace.data("elementId", element.getId());
        trace.data("propertyName", property.getName());
        trace.data("propertyKey", property.getKey());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            if (element instanceof Vertex) {
                addMutations(VertexiumObjectType.VERTEX, getMarkVisiblePropertyMutation(element.getId(), property, timestamp, columnVisibility));
            } else if (element instanceof Edge) {
                addMutations(VertexiumObjectType.EDGE, getMarkVisiblePropertyMutation(element.getId(), property, timestamp, columnVisibility));
            }

            getSearchIndex().markPropertyVisible(this, element, property, visibility, authorizations);

            if (hasEventListeners()) {
                fireGraphEvent(new MarkVisiblePropertyEvent(this, element, property, visibility));
            }
        } finally {
            trace.stop();
        }
    }

    private Mutation getMarkVisiblePropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility) {
        Mutation m = new Mutation(rowKey);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(property, getNameSubstitutionStrategy());
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, timestamp, AccumuloElement.HIDDEN_VALUE_DELETED);
        return m;
    }

    @Override
    public void flushGraph() {
        flushWriter(this.batchWriter);
    }

    @Override
    public void flush() {
        if (hasEventListeners()) {
            synchronized (this.graphEventQueue) {
                flushWritersAndSuper();
                flushGraphEventQueue();
            }
        } else {
            flushWritersAndSuper();
        }
    }

    private void flushWritersAndSuper() {
        flushWriter(this.batchWriter);
        super.flush();
    }

    private void flushGraphEventQueue() {
        GraphEvent graphEvent;
        while ((graphEvent = this.graphEventQueue.poll()) != null) {
            fireGraphEvent(graphEvent);
        }
    }

    private static void flushWriter(MultiTableBatchWriter writer) {
        if (writer == null) {
            return;
        }

        try {
            if (!writer.isClosed()) {
                writer.flush();
            }
        } catch (MutationsRejectedException e) {
            throw new VertexiumException("Unable to flush writer", e);
        }
    }

    @Override
    public void shutdown() {
        try {
            flush();
            super.shutdown();
            streamingPropertyValueStorageStrategy.close();
            this.graphMetadataStore.close();
            this.curatorFramework.close();
            this.batchWriter.close();
        } catch (Exception ex) {
            throw new VertexiumException(ex);
        }
    }

    private Mutation getDeleteRowMutation(String rowKey) {
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
        return m;
    }

    private Mutation getSoftDeleteRowMutation(String rowKey, long timestamp) {
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.CF_SOFT_DELETE, AccumuloElement.CQ_SOFT_DELETE, timestamp, AccumuloElement.SOFT_DELETE_VALUE);
        return m;
    }

    private Mutation getMarkHiddenRowMutation(String rowKey, ColumnVisibility visibility) {
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, visibility, AccumuloElement.HIDDEN_VALUE);
        return m;
    }

    private Mutation getMarkVisibleRowMutation(String rowKey, ColumnVisibility visibility) {
        Mutation m = new Mutation(rowKey);
        m.putDelete(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, visibility);
        return m;
    }

    public VertexiumSerializer getVertexiumSerializer() {
        return vertexiumSerializer;
    }

    @Override
    public AccumuloGraphConfiguration getConfiguration() {
        return (AccumuloGraphConfiguration) super.getConfiguration();
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations) throws VertexiumException {
        try {
            if (vertexId == null) {
                return null;
            }

            Span trace = Trace.start("getVertex");
            trace.data("vertexId", vertexId);
            traceDataFetchHints(trace, fetchHints);
            return singleOrDefault(getVerticesInRange(trace, new org.apache.accumulo.core.data.Range(vertexId), fetchHints, endTime, authorizations), null);
        } catch (IllegalStateException ex) {
            throw new VertexiumException("Failed to find vertex with id: " + vertexId, ex);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityVertexiumException("Could not get vertex " + vertexId + " with authorizations: " + authorizations, authorizations, ex.getCause());
            }
            throw ex;
        }
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Span trace = Trace.start("getVerticesWithPrefix");
        trace.data("vertexIdPrefix", vertexIdPrefix);
        traceDataFetchHints(trace, fetchHints);
        org.apache.accumulo.core.data.Range range = org.apache.accumulo.core.data.Range.prefix(vertexIdPrefix);
        return getVerticesInRange(trace, range, fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Span trace = Trace.start("getVerticesInRange");
        trace.data("rangeInclusiveStart", idRange.getInclusiveStart());
        trace.data("rangeExclusiveStart", idRange.getExclusiveEnd());
        traceDataFetchHints(trace, fetchHints);
        org.apache.accumulo.core.data.Range range = vertexiumRangeToAccumuloRange(idRange);
        return getVerticesInRange(trace, range, fetchHints, endTime, authorizations);
    }

    private CloseableIterable<Vertex> getVerticesInRange(
            Span trace,
            String startId,
            String endId,
            FetchHints fetchHints,
            Long timestamp,
            final Authorizations authorizations
    ) throws VertexiumException {
        trace.data("startId", startId);
        trace.data("endId", endId);
        if (Trace.isTracing() && timestamp != null) {
            trace.data("timestamp", Long.toString(timestamp));
        }
        traceDataFetchHints(trace, fetchHints);

        final Key startKey;
        if (startId == null) {
            startKey = null;
        } else {
            startKey = new Key(startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = null;
        } else {
            endKey = new Key(endId).followingKey(PartialKey.ROW);
        }

        org.apache.accumulo.core.data.Range range = new org.apache.accumulo.core.data.Range(startKey, endKey);
        return getVerticesInRange(trace, range, fetchHints, timestamp, authorizations);
    }

    protected ScannerBase createVertexScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(fetchHints, ElementType.VERTEX, maxVersions, startTime, endTime, Lists.newArrayList(range), authorizations);
    }

    protected ScannerBase createEdgeScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(fetchHints, ElementType.EDGE, maxVersions, startTime, endTime, Lists.newArrayList(range), authorizations);
    }

    private ScannerBase createElementScanner(
            FetchHints fetchHints,
            ElementType elementType,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(fetchHints, elementType, maxVersions, startTime, endTime, ranges, true, authorizations);
    }

    ScannerBase createElementScanner(
            FetchHints fetchHints,
            ElementType elementType,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            boolean useVertexiumElementIterators,
            Authorizations authorizations
    ) throws VertexiumException {
        try {
            String tableName;
            if (isHistoryInSeparateTable() && (startTime != null || endTime != null || maxVersions == null || maxVersions > 1)) {
                tableName = getHistoryTableNameFromElementType(elementType);
            } else {
                tableName = getTableNameFromElementType(elementType);
            }
            ScannerBase scanner;
            if (ranges == null || ranges.size() == 1) {
                org.apache.accumulo.core.data.Range range = ranges == null ? null : ranges.iterator().next();
                scanner = createScanner(tableName, range, authorizations);
            } else {
                scanner = createBatchScanner(tableName, ranges, authorizations);
            }

            if (startTime != null || endTime != null) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                        80,
                        TimestampFilter.class.getSimpleName(),
                        TimestampFilter.class
                );
                if (startTime != null) {
                    TimestampFilter.setStart(iteratorSetting, startTime, true);
                }
                if (endTime != null) {
                    TimestampFilter.setEnd(iteratorSetting, endTime, true);
                }
                scanner.addScanIterator(iteratorSetting);
            }

            if (maxVersions != null) {
                IteratorSetting versioningIteratorSettings = new IteratorSetting(
                        90,
                        VersioningIterator.class.getSimpleName(),
                        VersioningIterator.class
                );
                VersioningIterator.setMaxVersions(versioningIteratorSettings, maxVersions);
                scanner.addScanIterator(versioningIteratorSettings);
            }

            if (useVertexiumElementIterators) {
                if (elementType == ElementType.VERTEX) {
                    IteratorSetting vertexIteratorSettings = new IteratorSetting(
                            1000,
                            VertexIterator.class.getSimpleName(),
                            VertexIterator.class
                    );
                    VertexIterator.setFetchHints(vertexIteratorSettings, toIteratorFetchHints(fetchHints));
                    scanner.addScanIterator(vertexIteratorSettings);
                } else if (elementType == ElementType.EDGE) {
                    IteratorSetting edgeIteratorSettings = new IteratorSetting(
                            1000,
                            EdgeIterator.class.getSimpleName(),
                            EdgeIterator.class
                    );
                    EdgeIterator.setFetchHints(edgeIteratorSettings, toIteratorFetchHints(fetchHints));
                    scanner.addScanIterator(edgeIteratorSettings);
                } else {
                    throw new VertexiumException("Unexpected element type: " + elementType);
                }
            }

            applyFetchHints(scanner, fetchHints, elementType);
            GRAPH_LOGGER.logStartIterator(scanner);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new VertexiumException(e);
        }
    }

    public IteratorFetchHints toIteratorFetchHints(FetchHints fetchHints) {
        return new IteratorFetchHints(
                fetchHints.isIncludeAllProperties(),
                deflate(fetchHints.getPropertyNamesToInclude()),
                fetchHints.isIncludeAllPropertyMetadata(),
                deflate(fetchHints.getMetadataKeysToInclude()),
                fetchHints.isIncludeHidden(),
                fetchHints.isIncludeAllEdgeRefs(),
                fetchHints.isIncludeOutEdgeRefs(),
                fetchHints.isIncludeInEdgeRefs(),
                deflate(fetchHints.getEdgeLabelsOfEdgeRefsToInclude()),
                fetchHints.isIncludeEdgeLabelsAndCounts(),
                fetchHints.isIncludeExtendedDataTableNames()
        );
    }

    private ImmutableSet<String> deflate(ImmutableSet<String> strings) {
        if (strings == null) {
            return null;
        }
        return ImmutableSet.copyOf(
                strings.stream()
                        .map(s -> getNameSubstitutionStrategy().deflate(s))
                        .collect(Collectors.toSet())
        );
    }

    protected ScannerBase createVertexScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(
                fetchHints,
                ElementType.VERTEX,
                maxVersions,
                startTime,
                endTime,
                ranges,
                authorizations
        );
    }

    protected ScannerBase createEdgeScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(
                fetchHints,
                ElementType.EDGE,
                maxVersions,
                startTime,
                endTime,
                ranges,
                authorizations
        );
    }

    public ScannerBase createBatchScanner(
            String tableName,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws TableNotFoundException {
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = toAccumuloAuthorizations(authorizations);
        return createBatchScanner(tableName, ranges, accumuloAuthorizations);
    }

    public ScannerBase createBatchScanner(
            String tableName,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            org.apache.accumulo.core.security.Authorizations accumuloAuthorizations
    ) throws TableNotFoundException {
        ScannerBase scanner;
        scanner = connector.createBatchScanner(tableName, accumuloAuthorizations, numberOfQueryThreads);
        ((BatchScanner) scanner).setRanges(ranges);
        return scanner;
    }

    private Scanner createScanner(
            String tableName,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws TableNotFoundException {
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = toAccumuloAuthorizations(authorizations);
        return createScanner(tableName, range, accumuloAuthorizations);
    }

    private Scanner createScanner(
            String tableName,
            org.apache.accumulo.core.data.Range range,
            org.apache.accumulo.core.security.Authorizations accumuloAuthorizations
    ) throws TableNotFoundException {
        Scanner scanner = connector.createScanner(tableName, accumuloAuthorizations);
        if (range != null) {
            scanner.setRange(range);
        }
        return scanner;
    }

    private void applyFetchHints(ScannerBase scanner, FetchHints fetchHints, ElementType elementType) {
        scanner.clearColumns();

        Iterable<Text> columnFamiliesToFetch = getColumnFamiliesToFetch(elementType, fetchHints);
        for (Text columnFamilyToFetch : columnFamiliesToFetch) {
            scanner.fetchColumnFamily(columnFamilyToFetch);
        }
    }

    public static Iterable<Text> getColumnFamiliesToFetch(ElementType elementType, FetchHints fetchHints) {
        List<Text> columnFamiliesToFetch = new ArrayList<>();

        columnFamiliesToFetch.add(AccumuloElement.CF_HIDDEN);
        columnFamiliesToFetch.add(AccumuloElement.CF_SOFT_DELETE);
        columnFamiliesToFetch.add(AccumuloElement.DELETE_ROW_COLUMN_FAMILY);

        if (elementType == ElementType.VERTEX) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_SIGNAL);
        } else if (elementType == ElementType.EDGE) {
            columnFamiliesToFetch.add(AccumuloEdge.CF_SIGNAL);
            columnFamiliesToFetch.add(AccumuloEdge.CF_IN_VERTEX);
            columnFamiliesToFetch.add(AccumuloEdge.CF_OUT_VERTEX);
        } else {
            throw new VertexiumException("Unhandled element type: " + elementType);
        }

        if (fetchHints.isIncludeAllEdgeRefs()
                || fetchHints.isIncludeInEdgeRefs()
                || fetchHints.isIncludeEdgeLabelsAndCounts()
                || fetchHints.hasEdgeLabelsOfEdgeRefsToInclude()) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE);
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE_HIDDEN);
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE_SOFT_DELETE);
        }
        if (fetchHints.isIncludeAllEdgeRefs()
                || fetchHints.isIncludeOutEdgeRefs()
                || fetchHints.isIncludeEdgeLabelsAndCounts()
                || fetchHints.hasEdgeLabelsOfEdgeRefsToInclude()) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE);
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE_HIDDEN);
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE);
        }
        if (fetchHints.isIncludeProperties()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
        }
        if (fetchHints.isIncludePropertyMetadata()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_METADATA);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
        }
        if (fetchHints.isIncludeExtendedDataTableNames()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_EXTENDED_DATA);
        }

        return columnFamiliesToFetch;
    }

    public String getTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return getVerticesTableName();
            case EDGE:
                return getEdgesTableName();
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    public String getHistoryTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return getHistoryVerticesTableName();
            case EDGE:
                return getHistoryEdgesTableName();
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    public org.apache.accumulo.core.security.Authorizations toAccumuloAuthorizations(Authorizations authorizations) {
        if (authorizations == null) {
            throw new NullPointerException("authorizations is required");
        }
        return new org.apache.accumulo.core.security.Authorizations(authorizations.getAuthorizations());
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Span trace = Trace.start("getEdge");
        trace.data("edgeId", edgeId);
        try {
            return singleOrDefault(getEdgesInRange(trace, edgeId, edgeId, fetchHints, endTime, authorizations), null);
        } catch (IllegalStateException ex) {
            throw new VertexiumException("Failed to find edge with id: " + edgeId, ex);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityVertexiumException("Could not get edge " + edgeId + " with authorizations: " + authorizations, authorizations, ex.getCause());
            }
            throw ex;
        }
    }

    public static ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    public static ColumnVisibility visibilityToAccumuloVisibility(String visibilityString) {
        return new ColumnVisibility(visibilityString);
    }

    public static Visibility accumuloVisibilityToVisibility(ColumnVisibility columnVisibility) {
        if (columnVisibility.equals(EMPTY_COLUMN_VISIBILITY)) {
            return Visibility.EMPTY;
        }
        String columnVisibilityString = columnVisibility.toString();
        return accumuloVisibilityToVisibility(columnVisibilityString);
    }

    public static Visibility accumuloVisibilityToVisibility(Text columnVisibility) {
        return accumuloVisibilityToVisibility(columnVisibility.toString());
    }

    public static Visibility accumuloVisibilityToVisibility(String columnVisibilityString) {
        if (columnVisibilityString.startsWith("[") && columnVisibilityString.endsWith("]")) {
            if (columnVisibilityString.length() == 2) {
                return Visibility.EMPTY;
            }
            columnVisibilityString = columnVisibilityString.substring(1, columnVisibilityString.length() - 1);
        }
        if (columnVisibilityString.length() == 0) {
            return Visibility.EMPTY;
        }
        return new Visibility(columnVisibilityString);
    }

    public static String getVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_v";
    }

    public static String getHistoryVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_vh";
    }

    public static String getEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_e");
    }

    public static String getHistoryEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_eh");
    }

    public static String getExtendedDataTableName(String tableNamePrefix) {
        return tableNamePrefix + "_extdata";
    }

    public static String getDataTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_d");
    }

    public static String getMetadataTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_m");
    }

    public String getVerticesTableName() {
        return verticesTableName;
    }

    public String getHistoryVerticesTableName() {
        return historyVerticesTableName;
    }

    public String getEdgesTableName() {
        return edgesTableName;
    }

    public String getHistoryEdgesTableName() {
        return historyEdgesTableName;
    }

    public String getExtendedDataTableName() {
        return extendedDataTableName;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public StreamingPropertyValueStorageStrategy getStreamingPropertyValueStorageStrategy() {
        return streamingPropertyValueStorageStrategy;
    }

    public AccumuloGraphLogger getGraphLogger() {
        return GRAPH_LOGGER;
    }

    public Connector getConnector() {
        return connector;
    }

    public Iterable<Range> listVerticesTableSplits() {
        return listTableSplits(getVerticesTableName());
    }

    public Iterable<Range> listHistoryVerticesTableSplits() {
        return listTableSplits(getHistoryVerticesTableName());
    }

    public Iterable<Range> listEdgesTableSplits() {
        return listTableSplits(getEdgesTableName());
    }

    public Iterable<Range> listHistoryEdgesTableSplits() {
        return listTableSplits(getHistoryEdgesTableName());
    }

    public Iterable<Range> listDataTableSplits() {
        return listTableSplits(getDataTableName());
    }

    public Iterable<Range> listExtendedDataTableSplits() {
        return listTableSplits(getExtendedDataTableName());
    }

    private Iterable<Range> listTableSplits(String tableName) {
        try {
            return splitsIterableToRangeIterable(getConnector().tableOperations().listSplits(tableName));
        } catch (Exception ex) {
            throw new VertexiumException("Could not get splits for: " + tableName, ex);
        }
    }

    private Iterable<Range> splitsIterableToRangeIterable(final Iterable<Text> splits) {
        String inclusiveStart = null;
        List<Range> ranges = new ArrayList<>();
        for (Text split : splits) {
            String exclusiveEnd = new Key(split).getRow().toString();
            ranges.add(new Range(inclusiveStart, exclusiveEnd));
            inclusiveStart = exclusiveEnd;
        }
        ranges.add(new Range(inclusiveStart, null));
        return ranges;
    }

    void alterElementVisibility(AccumuloElement element, Visibility newVisibility) {
        String elementRowKey = element.getId();
        Span trace = Trace.start("alterElementVisibility");
        trace.data("elementRowKey", elementRowKey);
        try {
            if (element instanceof Edge) {
                Edge edge = (Edge) element;

                String vertexOutRowKey = edge.getVertexId(Direction.OUT);
                Mutation vertexOutMutation = new Mutation(vertexOutRowKey);
                if (elementMutationBuilder.alterEdgeVertexOutVertex(vertexOutMutation, edge, newVisibility)) {
                    addMutations(VertexiumObjectType.VERTEX, vertexOutMutation);
                }

                String vertexInRowKey = edge.getVertexId(Direction.IN);
                Mutation vertexInMutation = new Mutation(vertexInRowKey);
                if (elementMutationBuilder.alterEdgeVertexInVertex(vertexInMutation, edge, newVisibility)) {
                    addMutations(VertexiumObjectType.VERTEX, vertexInMutation);
                }
            }

            Mutation m = new Mutation(elementRowKey);
            if (elementMutationBuilder.alterElementVisibility(m, element, newVisibility)) {
                addMutations(element, m);
            }
            element.setVisibility(newVisibility);
        } finally {
            trace.stop();
        }
    }

    public void alterEdgeLabel(AccumuloEdge edge, String newEdgeLabel) {
        elementMutationBuilder.alterEdgeLabel(edge, newEdgeLabel);
    }

    void alterElementPropertyVisibilities(AccumuloElement element, List<AlterPropertyVisibility> alterPropertyVisibilities) {
        if (alterPropertyVisibilities.size() == 0) {
            return;
        }

        String elementRowKey = element.getId();

        Mutation m = new Mutation(elementRowKey);

        List<PropertyDescriptor> propertyList = Lists.newArrayList();
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            MutableProperty property = (MutableProperty) element.getProperty(
                    apv.getKey(),
                    apv.getName(),
                    apv.getExistingVisibility()
            );
            if (property == null) {
                throw new VertexiumException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (property.getVisibility().equals(apv.getVisibility())) {
                continue;
            }
            if (apv.getExistingVisibility() == null) {
                apv.setExistingVisibility(property.getVisibility());
            }
            elementMutationBuilder.addPropertySoftDeleteToMutation(m, property);
            property.setVisibility(apv.getVisibility());
            property.setTimestamp(apv.getTimestamp());
            elementMutationBuilder.addPropertyToMutation(this, m, elementRowKey, property);

            // Keep track of properties that need to be removed from indices
            propertyList.add(PropertyDescriptor.from(apv.getKey(), apv.getName(), apv.getExistingVisibility()));
        }


        if (!propertyList.isEmpty()) {
            addMutations(element, m);
        }
    }

    void alterPropertyMetadatas(AccumuloElement element, List<SetPropertyMetadata> setPropertyMetadatas) {
        if (setPropertyMetadatas.size() == 0) {
            return;
        }

        String elementRowKey = element.getId();
        Mutation m = new Mutation(elementRowKey);
        for (SetPropertyMetadata apm : setPropertyMetadatas) {
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new VertexiumException(String.format("Could not find property %s:%s(%s)", apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility()));
            }
            if (property.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
                property.getMetadata().add(apm.getMetadataName(), apm.getNewValue(), apm.getMetadataVisibility());
            }
            elementMutationBuilder.addPropertyMetadataItemToMutation(
                    m,
                    property,
                    apm.getMetadataName(),
                    apm.getNewValue(),
                    apm.getMetadataVisibility()
            );
        }

        addMutations(element, m);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }

    private boolean isHistoryInSeparateTable() {
        return historyInSeparateTable;
    }

    @Override
    public void truncate() {
        try {
            this.connector.tableOperations().deleteRows(getDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getEdgesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getVerticesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getExtendedDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getMetadataTableName(), null, null);
            if (isHistoryInSeparateTable()) {
                this.connector.tableOperations().deleteRows(getHistoryEdgesTableName(), null, null);
                this.connector.tableOperations().deleteRows(getHistoryVerticesTableName(), null, null);
            }
            getSearchIndex().truncate(this);
        } catch (Exception ex) {
            throw new VertexiumException("Could not delete rows", ex);
        }
    }

    @Override
    public void drop() {
        try {
            dropTableIfExists(getDataTableName());
            dropTableIfExists(getEdgesTableName());
            dropTableIfExists(getVerticesTableName());
            dropTableIfExists(getMetadataTableName());
            if (isHistoryInSeparateTable()) {
                dropTableIfExists(getHistoryEdgesTableName());
                dropTableIfExists(getHistoryVerticesTableName());
            }
            getSearchIndex().drop(this);
        } catch (Exception ex) {
            throw new VertexiumException("Could not drop tables", ex);
        }
    }

    private void dropTableIfExists(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (this.connector.tableOperations().exists(tableName)) {
            this.connector.tableOperations().delete(tableName);
        }
    }

    @Override
    public Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        Set<String> vertexIdsSet = IterableUtils.toSet(vertexIds);
        Span trace = Trace.start("findRelatedEdges");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("findRelatedEdges:\n  %s", IterableUtils.join(vertexIdsSet, "\n  "));
            }

            if (vertexIdsSet.size() == 0) {
                return new HashSet<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            Long startTime = null;
            int maxVersions = 1;
            FetchHints fetchHints = FetchHints.builder()
                    .setIncludeOutEdgeRefs(true)
                    .build();
            ScannerBase scanner = createElementScanner(
                    fetchHints,
                    ElementType.VERTEX,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting edgeRefFilterSettings = new IteratorSetting(
                    1000,
                    EdgeRefFilter.class.getSimpleName(),
                    EdgeRefFilter.class
            );
            EdgeRefFilter.setVertexIds(edgeRefFilterSettings, vertexIdsSet);
            scanner.addScanIterator(edgeRefFilterSettings);

            IteratorSetting vertexEdgeIdIteratorSettings = new IteratorSetting(
                    1001,
                    VertexEdgeIdIterator.class.getSimpleName(),
                    VertexEdgeIdIterator.class
            );
            scanner.addScanIterator(vertexEdgeIdIteratorSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
                List<String> edgeIds = new ArrayList<>();
                while (it.hasNext()) {
                    Map.Entry<Key, Value> c = it.next();
                    for (ByteArrayWrapper edgeId : VertexEdgeIdIterator.decodeValue(c.getValue())) {
                        edgeIds.add(new Text(edgeId.getData()).toString());
                    }
                }
                return edgeIds;
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        Set<String> vertexIdsSet = IterableUtils.toSet(vertexIds);
        Span trace = Trace.start("findRelatedEdgeSummary");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("findRelatedEdgeSummary:\n  %s", IterableUtils.join(vertexIdsSet, "\n  "));
            }

            if (vertexIdsSet.size() == 0) {
                return new ArrayList<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            Long startTime = null;
            int maxVersions = 1;
            FetchHints fetchHints = FetchHints.builder()
                    .setIncludeOutEdgeRefs(true)
                    .build();
            ScannerBase scanner = createElementScanner(
                    fetchHints,
                    ElementType.VERTEX,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting edgeRefFilterSettings = new IteratorSetting(
                    1000,
                    EdgeRefFilter.class.getSimpleName(),
                    EdgeRefFilter.class
            );
            EdgeRefFilter.setVertexIds(edgeRefFilterSettings, vertexIdsSet);
            scanner.addScanIterator(edgeRefFilterSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                List<RelatedEdge> results = new ArrayList<>();
                List<String> softDeletedEdgeIds = new ArrayList<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    Text columnFamily = row.getKey().getColumnFamily();
                    if (!columnFamily.equals(AccumuloVertex.CF_OUT_EDGE)) {
                        if (columnFamily.equals(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE) || columnFamily.equals(AccumuloVertex.CF_OUT_EDGE_HIDDEN)) {
                            softDeletedEdgeIds.add(row.getKey().getColumnQualifier().toString());
                            results.removeIf(relatedEdge -> softDeletedEdgeIds.contains(relatedEdge.getEdgeId()));
                        }
                        continue;
                    }
                    org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo
                            = new EdgeInfo(row.getValue().get(), row.getKey().getTimestamp());
                    String edgeId = row.getKey().getColumnQualifier().toString();
                    String outVertexId = row.getKey().getRow().toString();
                    String inVertexId = edgeInfo.getVertexId();
                    String label = getNameSubstitutionStrategy().inflate(edgeInfo.getLabel());
                    if (!softDeletedEdgeIds.contains(edgeId)) {
                        results.add(new RelatedEdgeImpl(edgeId, label, outVertexId, inVertexId));
                    }
                }
                return results;
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Iterable<Path> findPaths(FindPathOptions options, Authorizations authorizations) {
        ProgressCallback progressCallback = options.getProgressCallback();
        if (progressCallback == null) {
            progressCallback = new ProgressCallback() {
                @Override
                public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                    LOGGER.debug("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount));
                }
            };
        }

        return new AccumuloFindPathStrategy(this, options, progressCallback, authorizations).findPaths();
    }

    @Override
    public Iterable<String> filterEdgeIdsByAuthorization(Iterable<String> edgeIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations) {
        return filterElementIdsByAuthorization(
                ElementType.EDGE,
                edgeIds,
                authorizationToMatch,
                filters,
                authorizations
        );
    }

    @Override
    public Iterable<String> filterVertexIdsByAuthorization(Iterable<String> vertexIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations) {
        return filterElementIdsByAuthorization(
                ElementType.VERTEX,
                vertexIds,
                authorizationToMatch,
                filters,
                authorizations
        );
    }

    private Iterable<String> filterElementIdsByAuthorization(ElementType elementType, Iterable<String> elementIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations) {
        Set<String> elementIdsSet = IterableUtils.toSet(elementIds);
        Span trace = Trace.start("filterElementIdsByAuthorization");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("filterElementIdsByAuthorization:\n  %s", IterableUtils.join(elementIdsSet, "\n  "));
            }

            if (elementIdsSet.size() == 0) {
                return new ArrayList<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String elementId : elementIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(elementId));
            }

            Long startTime = null;
            Long endTime = null;
            int maxVersions = 1;
            ScannerBase scanner = createElementScanner(
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    elementType,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting hasAuthorizationFilterSettings = new IteratorSetting(
                    1000,
                    HasAuthorizationFilter.class.getSimpleName(),
                    HasAuthorizationFilter.class
            );
            HasAuthorizationFilter.setAuthorizationToMatch(hasAuthorizationFilterSettings, authorizationToMatch);
            HasAuthorizationFilter.setFilters(hasAuthorizationFilterSettings, filters);
            scanner.addScanIterator(hasAuthorizationFilterSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Set<String> results = new HashSet<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    results.add(row.getKey().getRow().toString());
                }
                return results;
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    public Iterable<GraphMetadataEntry> getMetadataInRange(final org.apache.accumulo.core.data.Range range) {
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, GraphMetadataEntry>() {
            public Scanner scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, GraphMetadataEntry graphMetadataEntry) {
                return true;
            }

            @Override
            protected GraphMetadataEntry convert(Map.Entry<Key, Value> entry) {
                String key = entry.getKey().getRow().toString();
                return new GraphMetadataEntry(key, entry.getValue().get());
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = createScanner(getMetadataTableName(), range, METADATA_AUTHORIZATIONS);
                    GRAPH_LOGGER.logStartIterator(scanner);

                    IteratorSetting versioningIteratorSettings = new IteratorSetting(
                            90,
                            VersioningIterator.class.getSimpleName(),
                            VersioningIterator.class
                    );
                    VersioningIterator.setMaxVersions(versioningIteratorSettings, 1);
                    scanner.addScanIterator(versioningIteratorSettings);

                    return scanner.iterator();
                } catch (TableNotFoundException ex) {
                    throw new VertexiumException("Could not create metadata scanner", ex);
                }
            }

            @Override
            public void close() {
                super.close();
                this.scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return graphMetadataStore;
    }

    private CloseableIterable<ExtendedDataRow> getExtendedDataRowsInRange(
            Span trace,
            List<org.apache.accumulo.core.data.Range> ranges,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, ExtendedDataRow>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, ExtendedDataRow dest) {
                return dest != null;
            }

            @Override
            protected ExtendedDataRow convert(Map.Entry<Key, Value> next) {
                try {
                    SortedMap<Key, Value> row = WholeRowIterator.decodeRow(next.getKey(), next.getValue());
                    ExtendedDataRowId extendedDataRowId = KeyHelper.parseExtendedDataRowId(next.getKey().getRow());
                    return new AccumuloExtendedDataRow(extendedDataRowId, row, fetchHints, vertexiumSerializer);
                } catch (IOException e) {
                    throw new VertexiumException("Could not decode row", e);
                }
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = createExtendedDataRowScanner(ranges, authorizations);
                    return scanner.iterator();
                } catch (RuntimeException ex) {
                    if (ex.getCause() instanceof AccumuloSecurityException) {
                        throw new SecurityVertexiumException("Could not get vertices with authorizations: " + authorizations, authorizations, ex.getCause());
                    }
                    throw ex;
                }
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                if (trace != null) {
                    trace.stop();
                }
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    private ScannerBase createExtendedDataRowScanner(List<org.apache.accumulo.core.data.Range> ranges, Authorizations authorizations) {
        try {
            String tableName = getExtendedDataTableName();
            ScannerBase scanner;
            if (ranges == null || ranges.size() == 1) {
                org.apache.accumulo.core.data.Range range = ranges == null ? null : ranges.iterator().next();
                scanner = createScanner(tableName, range, authorizations);
            } else {
                scanner = createBatchScanner(tableName, ranges, authorizations);
            }

            IteratorSetting versioningIteratorSettings = new IteratorSetting(
                    90, // versioning needs to happen before combining into one row
                    VersioningIterator.class.getSimpleName(),
                    VersioningIterator.class
            );
            VersioningIterator.setMaxVersions(versioningIteratorSettings, 1);
            scanner.addScanIterator(versioningIteratorSettings);

            IteratorSetting rowIteratorSettings = new IteratorSetting(
                    100,
                    WholeRowIterator.class.getSimpleName(),
                    WholeRowIterator.class
            );
            scanner.addScanIterator(rowIteratorSettings);

            GRAPH_LOGGER.logStartIterator(scanner);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new VertexiumException(e);
        }
    }

    protected CloseableIterable<Vertex> getVerticesInRange(
            final Span trace,
            final org.apache.accumulo.core.data.Range range,
            final FetchHints fetchHints,
            final Long endTime,
            final Authorizations authorizations
    ) {
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Vertex>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Map.Entry<Key, Value> next) {
                return createVertexFromVertexIteratorValue(next.getKey(), next.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = createVertexScanner(fetchHints, SINGLE_VERSION, null, endTime, range, authorizations);
                    return scanner.iterator();
                } catch (RuntimeException ex) {
                    if (ex.getCause() instanceof AccumuloSecurityException) {
                        throw new SecurityVertexiumException("Could not get vertices with authorizations: " + authorizations, authorizations, ex.getCause());
                    }
                    throw ex;
                }
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                if (trace != null) {
                    trace.stop();
                }
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    private Vertex createVertexFromVertexIteratorValue(Key key, Value value, FetchHints fetchHints, Authorizations authorizations) {
        return AccumuloVertex.createFromIteratorValue(this, key, value, fetchHints, authorizations);
    }

    private Edge createEdgeFromEdgeIteratorValue(Key key, Value value, FetchHints fetchHints, Authorizations authorizations) {
        return AccumuloEdge.createFromIteratorValue(this, key, value, fetchHints, authorizations);
    }

    @Override
    public CloseableIterable<Vertex> getVertices(Iterable<String> ids, final FetchHints fetchHints, final Long endTime, final Authorizations authorizations) {
        final List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
        int idCount = 0;
        for (String id : ids) {
            ranges.add(RangeUtils.createRangeFromString(id));
            idCount++;
        }
        if (ranges.size() == 0) {
            return new EmptyClosableIterable<>();
        }

        final Span trace = Trace.start("getVertices");
        trace.data("idCount", Integer.toString(idCount));
        traceDataFetchHints(trace, fetchHints);
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Vertex>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Map.Entry<Key, Value> row) {
                return createVertexFromVertexIteratorValue(row.getKey(), row.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                Long startTime = null;
                scanner = createVertexScanner(fetchHints, 1, startTime, endTime, ranges, authorizations);
                return scanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                trace.stop();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    @Override
    public CloseableIterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        final List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
        int idCount = 0;
        for (String id : ids) {
            ranges.add(RangeUtils.createRangeFromString(id));
            idCount++;
        }
        if (ranges.size() == 0) {
            return new EmptyClosableIterable<>();
        }

        final Span trace = Trace.start("getEdges");
        trace.data("idCount", Integer.toString(idCount));
        traceDataFetchHints(trace, fetchHints);
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Edge>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Map.Entry<Key, Value> row) {
                return createEdgeFromEdgeIteratorValue(row.getKey(), row.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                Long startTime = null;
                scanner = createEdgeScanner(fetchHints, 1, startTime, endTime, ranges, authorizations);
                return scanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                trace.stop();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    @Override
    public Iterable<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Span trace = Trace.start("getEdgesInRange");
        trace.data("rangeInclusiveStart", idRange.getInclusiveStart());
        trace.data("rangeExclusiveStart", idRange.getExclusiveEnd());
        traceDataFetchHints(trace, fetchHints);
        org.apache.accumulo.core.data.Range range = vertexiumRangeToAccumuloRange(idRange);
        return getEdgesInRange(trace, range, fetchHints, endTime, authorizations);
    }

    private org.apache.accumulo.core.data.Range vertexiumRangeToAccumuloRange(Range range) {
        Key inclusiveStartRow = range.getInclusiveStart() == null ? null : new Key(range.getInclusiveStart());
        Key exclusiveEndRow = range.getExclusiveEnd() == null ? null : new Key(range.getExclusiveEnd());
        boolean startKeyInclusive = true;
        boolean endKeyInclusive = false;
        return new org.apache.accumulo.core.data.Range(
                inclusiveStartRow, startKeyInclusive,
                exclusiveEndRow, endKeyInclusive
        );
    }

    protected CloseableIterable<Edge> getEdgesInRange(
            final Span trace,
            String startId,
            String endId,
            final FetchHints fetchHints,
            final Long timestamp,
            final Authorizations authorizations
    ) throws VertexiumException {
        trace.data("startId", startId);
        trace.data("endId", endId);
        if (Trace.isTracing() && timestamp != null) {
            trace.data("timestamp", Long.toString(timestamp));
        }
        traceDataFetchHints(trace, fetchHints);

        final Key startKey;
        if (startId == null) {
            startKey = null;
        } else {
            startKey = new Key(startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = null;
        } else {
            endKey = new Key(endId).followingKey(PartialKey.ROW);
        }

        org.apache.accumulo.core.data.Range range = new org.apache.accumulo.core.data.Range(startKey, endKey);
        return getEdgesInRange(trace, range, fetchHints, timestamp, authorizations);
    }

    protected CloseableIterable<Edge> getEdgesInRange(
            Span trace,
            org.apache.accumulo.core.data.Range range,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) throws VertexiumException {
        traceDataFetchHints(trace, fetchHints);

        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Edge>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Map.Entry<Key, Value> next) {
                return createEdgeFromEdgeIteratorValue(next.getKey(), next.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                scanner = createEdgeScanner(fetchHints, SINGLE_VERSION, null, endTime, range, authorizations);
                return scanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                if (trace != null) {
                    trace.stop();
                }
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    @Override
    public long getVertexCount(Authorizations authorizations) {
        String tableName = getTableNameFromElementType(ElementType.VERTEX);
        return getRowCountFromTable(tableName, AccumuloVertex.CF_SIGNAL, authorizations);
    }

    @Override
    public long getEdgeCount(Authorizations authorizations) {
        String tableName = getTableNameFromElementType(ElementType.EDGE);
        return getRowCountFromTable(tableName, AccumuloEdge.CF_SIGNAL, authorizations);
    }

    private long getRowCountFromTable(String tableName, Text signalColumn, Authorizations authorizations) {
        try {
            LOGGER.debug("BEGIN getRowCountFromTable(%s)", tableName);
            Scanner scanner = createScanner(tableName, null, authorizations);
            try {
                scanner.fetchColumnFamily(signalColumn);

                IteratorSetting countingIterator = new IteratorSetting(
                        100,
                        CountingIterator.class.getSimpleName(),
                        CountingIterator.class
                );
                scanner.addScanIterator(countingIterator);

                GRAPH_LOGGER.logStartIterator(scanner);

                long count = 0;
                for (Map.Entry<Key, Value> entry : scanner) {
                    Long countForKey = LongCombiner.FIXED_LEN_ENCODER.decode(entry.getValue().get());
                    LOGGER.debug("getRowCountFromTable(%s): %s: %d", tableName, entry.getKey().getRow(), countForKey);
                    count += countForKey;
                }
                LOGGER.debug("getRowCountFromTable(%s): TOTAL: %d", tableName, count);
                return count;
            } finally {
                scanner.close();
            }
        } catch (TableNotFoundException ex) {
            throw new VertexiumException("Could not get count from table: " + tableName, ex);
        }
    }

    public void traceOn(String description) {
        traceOn(description, new HashMap<>());
    }

    @Override
    public void traceOn(String description, Map<String, String> data) {
        if (!distributedTraceEnabled) {
            try {
                ClientConfiguration conf = getConfiguration().getClientConfiguration();
                DistributedTrace.enable(null, AccumuloGraph.class.getSimpleName(), conf);
                distributedTraceEnabled = true;
            } catch (Exception e) {
                throw new VertexiumException("Could not enable DistributedTrace", e);
            }
        }
        if (Trace.isTracing()) {
            throw new VertexiumException("Trace already running");
        }
        Span span = Trace.on(description);
        for (Map.Entry<String, String> dataEntry : data.entrySet()) {
            span.data(dataEntry.getKey(), dataEntry.getValue());
        }

        LOGGER.info("Started trace '%s'", description);
    }

    public void traceOff() {
        if (!Trace.isTracing()) {
            throw new VertexiumException("No trace currently running");
        }
        Trace.off();
    }

    private void traceDataFetchHints(Span trace, FetchHints fetchHints) {
        if (Trace.isTracing()) {
            trace.data("fetchHints", fetchHints.toString());
        }
    }

    @Override
    protected Class<?> getValueType(Object value) {
        if (value instanceof StreamingPropertyValueTableRef) {
            return ((StreamingPropertyValueTableRef) value).getValueType();
        }
        return super.getValueType(value);
    }

    private class AccumuloGraphMetadataStore extends GraphMetadataStore {
        private final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraphMetadataStore.class);
        private final Pattern ZK_PATH_REPLACEMENT_PATTERN = Pattern.compile("[^a-zA-Z]+");
        private final CuratorFramework curatorFramework;
        private final String zkPath;
        private final TreeCache treeCache;
        private final Map<String, GraphMetadataEntry> entries = new HashMap<>();

        public AccumuloGraphMetadataStore(CuratorFramework curatorFramework, String zkPath) {
            this.zkPath = zkPath;
            this.curatorFramework = curatorFramework;
            this.treeCache = new TreeCache(curatorFramework, zkPath);
            this.treeCache.getListenable().addListener((client, event) -> {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("treeCache event, clearing cache");
                }
                synchronized (entries) {
                    entries.clear();
                }
                getSearchIndex().clearCache();
            });
            try {
                this.treeCache.start();
            } catch (Exception e) {
                throw new VertexiumException("Could not start metadata sync", e);
            }
        }

        public void close() {
            this.treeCache.close();
        }

        @Override
        public Iterable<GraphMetadataEntry> getMetadata() {
            synchronized (entries) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("getMetadata");
                }
                ensureMetadataLoaded();
                return toList(entries.values());
            }
        }

        private void ensureMetadataLoaded() {
            synchronized (entries) {
                if (entries.size() > 0) {
                    return;
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("metadata is stale... loading");
                }
                Iterable<GraphMetadataEntry> metadata = getMetadataInRange(null);
                for (GraphMetadataEntry graphMetadataEntry : metadata) {
                    entries.put(graphMetadataEntry.getKey(), graphMetadataEntry);
                }
            }
        }

        @Override
        public void setMetadata(String key, Object value) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("setMetadata: %s = %s", key, value);
            }
            try {
                Mutation m = new Mutation(key);
                byte[] valueBytes = JavaSerializableUtils.objectToBytes(value);
                m.put(AccumuloElement.METADATA_COLUMN_FAMILY, AccumuloElement.METADATA_COLUMN_QUALIFIER, new Value(valueBytes));
                BatchWriter writer = getMetadataWriter();
                writer.addMutation(m);
                flush();
            } catch (MutationsRejectedException ex) {
                throw new VertexiumException("Could not add metadata " + key, ex);
            }

            synchronized (entries) {
                entries.clear();
                try {
                    signalMetadataChange(key);
                } catch (Exception e) {
                    LOGGER.error("Could not notify other nodes via ZooKeeper", e);
                }
            }
        }

        private void signalMetadataChange(String key) throws Exception {
            String path = zkPath + "/" + ZK_PATH_REPLACEMENT_PATTERN.matcher(key).replaceAll("_");
            LOGGER.debug("signaling change to metadata via path: %s", path);
            byte[] data = Longs.toByteArray(IncreasingTime.currentTimeMillis());
            this.curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(path, data);
        }

        @Override
        public Object getMetadata(String key) {
            GraphMetadataEntry e;
            synchronized (entries) {
                ensureMetadataLoaded();
                e = entries.get(key);
                if (e == null) {
                    return null;
                }
                return e.getValue();
            }
        }
    }
}
