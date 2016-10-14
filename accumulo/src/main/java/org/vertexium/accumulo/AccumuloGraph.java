package org.vertexium.accumulo;

import com.google.common.collect.Lists;
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
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.CreateMode;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.PropertyColumnQualifier;
import org.vertexium.accumulo.iterator.model.PropertyMetadataColumnQualifier;
import org.vertexium.accumulo.iterator.util.ByteArrayWrapper;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.accumulo.util.RangeUtils;
import org.vertexium.event.*;
import org.vertexium.mutation.AlterPropertyVisibility;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.mutation.SetPropertyMetadata;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.search.IndexHint;
import org.vertexium.util.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;

import static org.vertexium.util.IterableUtils.singleOrDefault;
import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.Preconditions.checkNotNull;

public class AccumuloGraph extends GraphBaseWithSearchIndex implements Traceable {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraph.class);
    private static final AccumuloGraphLogger GRAPH_LOGGER = new AccumuloGraphLogger(QUERY_LOGGER);
    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    private static final Object addIteratorLock = new Object();
    private static final Integer METADATA_ACCUMULO_GRAPH_VERSION = 2;
    private static final String METADATA_ACCUMULO_GRAPH_VERSION_KEY = "accumulo.graph.version";
    private static final String METADATA_SERIALIZER = "accumulo.graph.serializer";
    private static final Authorizations METADATA_AUTHORIZATIONS = new AccumuloAuthorizations();
    public static final int SINGLE_VERSION = 1;
    public static final Integer ALL_VERSIONS = null;
    private static final int ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY = 20;
    private static final String ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME = "vers";
    private static final ColumnVisibility EMPTY_COLUMN_VISIBILITY = new ColumnVisibility();
    private static final String CLASSPATH_CONTEXT_NAME = "vertexium";
    private final Connector connector;
    private final VertexiumSerializer vertexiumSerializer;
    private final FileSystem fileSystem;
    private final String dataDir;
    private final CuratorFramework curatorFramework;
    private ThreadLocal<VertexiumMultiTableBatchWriter> elementWriter = new ThreadLocal<>();
    private ThreadLocal<BatchWriter> metadataWriter = new ThreadLocal<>();
    protected ElementMutationBuilder elementMutationBuilder;
    private final Queue<GraphEvent> graphEventQueue = new LinkedList<>();
    private Integer accumuloGraphVersion;
    private boolean foundVertexiumSerializerMetadata;
    private final AccumuloNameSubstitutionStrategy nameSubstitutionStrategy;
    private final String verticesTableName;
    private final String edgesTableName;
    private final String dataTableName;
    private final String metadataTableName;
    private final int numberOfQueryThreads;
    private AccumuloGraphMetadataStore graphMetadataStore;
    private boolean distributedTraceEnabled;

    protected AccumuloGraph(
            AccumuloGraphConfiguration config,
            Connector connector,
            FileSystem fileSystem
    ) {
        super(config);
        this.connector = connector;
        this.vertexiumSerializer = config.createSerializer(this);
        this.fileSystem = fileSystem;
        this.dataDir = config.getDataDir();
        this.nameSubstitutionStrategy = AccumuloNameSubstitutionStrategy.create(config.createSubstitutionStrategy(this));
        long maxStreamingPropertyValueTableDataSize = config.getMaxStreamingPropertyValueTableDataSize();
        this.elementMutationBuilder = new ElementMutationBuilder(fileSystem, vertexiumSerializer, maxStreamingPropertyValueTableDataSize, dataDir) {
            @Override
            protected void saveVertexMutation(Mutation m) {
                addMutations(getVerticesWriter(), m);
            }

            @Override
            protected void saveEdgeMutation(Mutation m) {
                addMutations(getEdgesWriter(), m);
            }

            @Override
            protected AccumuloNameSubstitutionStrategy getNameSubstitutionStrategy() {
                return AccumuloGraph.this.getNameSubstitutionStrategy();
            }

            @Override
            protected void saveDataMutation(Mutation dataMutation) {
                addMutations(getDataWriter(), dataMutation);
            }

            @Override
            @SuppressWarnings("unchecked")
            protected StreamingPropertyValueRef saveStreamingPropertyValue(String rowKey, Property property, StreamingPropertyValue propertyValue) {
                StreamingPropertyValueRef streamingPropertyValueRef = super.saveStreamingPropertyValue(rowKey, property, propertyValue);
                ((MutableProperty) property).setValue(streamingPropertyValueRef.toStreamingPropertyValue(AccumuloGraph.this));
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
        this.dataTableName = getDataTableName(getConfiguration().getTableNamePrefix());
        this.metadataTableName = getMetadataTableName(getConfiguration().getTableNamePrefix());
        this.numberOfQueryThreads = getConfiguration().getNumberOfQueryThreads();
    }

    public static AccumuloGraph create(AccumuloGraphConfiguration config) throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        Connector connector = config.createConnector();
        FileSystem fs = config.createFileSystem();
        ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
        ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
        ensureTableExists(connector, getDataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
        ensureTableExists(connector, getMetadataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
        ensureRowDeletingIteratorIsAttached(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getDataTableName(config.getTableNamePrefix()));
        AccumuloGraph graph = new AccumuloGraph(config, connector, fs);
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
            if (graphMetadataEntry.getValue() instanceof String) {
                String vertexiumSerializerClassName = (String) graphMetadataEntry.getValue();
                if (!vertexiumSerializerClassName.equals(vertexiumSerializer.getClass().getName())) {
                    throw new VertexiumException("Invalid " + METADATA_SERIALIZER + " expected " + vertexiumSerializerClassName + " found " + vertexiumSerializer.getClass().getName());
                }
                foundVertexiumSerializerMetadata = true;
            } else {
                throw new VertexiumException("Invalid " + METADATA_SERIALIZER + " expected string found " + graphMetadataEntry.getValue().getClass().getName());
            }
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
            throw new RuntimeException("Unable to create table " + tableName, e);
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
    public static AccumuloGraph create(Map config) throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
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
                    }

                    if (hasEventListeners()) {
                        queueEvent(new AddVertexEvent(AccumuloGraph.this, vertex));
                        for (Property property : getProperties()) {
                            queueEvent(new AddPropertyEvent(AccumuloGraph.this, vertex, property));
                        }
                        for (PropertyDeleteMutation propertyDeleteMutation : getPropertyDeletes()) {
                            queueEvent(new DeletePropertyEvent(AccumuloGraph.this, vertex, propertyDeleteMutation));
                        }
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
                        getVertexId(),
                        getVisibility(),
                        getProperties(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        hiddenVisibilities,
                        timestampLong,
                        authorizations
                );
            }
        };
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
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
            IndexHint indexHint,
            Authorizations authorizations
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
            addMutations(getWriterFromElementType(element), m);
        }

        if (indexHint != IndexHint.DO_NOT_INDEX) {
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeletes) {
                getSearchIndex().deleteProperty(
                        this,
                        element,
                        propertyDeleteMutation.getKey(),
                        propertyDeleteMutation.getName(),
                        propertyDeleteMutation.getVisibility(),
                        authorizations
                );
            }
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeletes) {
                getSearchIndex().deleteProperty(
                        this,
                        element,
                        propertySoftDeleteMutation.getKey(),
                        propertySoftDeleteMutation.getName(),
                        propertySoftDeleteMutation.getVisibility(),
                        authorizations
                );
            }
            getSearchIndex().addElement(this, element, authorizations);
        }

        if (hasEventListeners()) {
            for (Property property : properties) {
                queueEvent(new AddPropertyEvent(AccumuloGraph.this, element, property));
            }
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeletes) {
                queueEvent(new DeletePropertyEvent(AccumuloGraph.this, element, propertyDeleteMutation));
            }
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeletes) {
                queueEvent(new SoftDeletePropertyEvent(AccumuloGraph.this, element, propertySoftDeleteMutation));
            }
        }
    }

    void deleteProperty(AccumuloElement element, Property property, Authorizations authorizations) {
        Mutation m = new Mutation(element.getId());
        elementMutationBuilder.addPropertyDeleteToMutation(m, property);
        addMutations(getWriterFromElementType(element), m);

        getSearchIndex().deleteProperty(this, element, property, authorizations);

        if (hasEventListeners()) {
            queueEvent(new DeletePropertyEvent(this, element, property));
        }
    }

    void softDeleteProperty(AccumuloElement element, Property property, Authorizations authorizations) {
        Mutation m = new Mutation(element.getId());
        elementMutationBuilder.addPropertySoftDeleteToMutation(m, property);
        addMutations(getWriterFromElementType(element), m);

        getSearchIndex().deleteProperty(this, element, property, authorizations);

        if (hasEventListeners()) {
            queueEvent(new SoftDeletePropertyEvent(this, element, property));
        }
    }

    protected void addMutations(BatchWriter writer, Mutation... mutations) {
        try {
            for (Mutation mutation : mutations) {
                writer.addMutation(mutation);
            }
            if (getConfiguration().isAutoFlush()) {
                flush();
            }
        } catch (MutationsRejectedException ex) {
            throw new RuntimeException("Could not add mutation", ex);
        }
    }

    private BatchWriter getElementWriter(String tableName) {
        if (elementWriter.get() == null) {
            BatchWriterConfig writerConfig = getConfiguration().createBatchWriterConfig();
            elementWriter.set(new VertexiumMultiTableBatchWriter(connector.createMultiTableBatchWriter(writerConfig)));
        }
        return elementWriter.get().getBatchWriter(tableName);
    }

    protected BatchWriter getVerticesWriter() {
        return getElementWriter(getVerticesTableName());
    }

    protected BatchWriter getEdgesWriter() {
        return getElementWriter(getEdgesTableName());
    }

    protected BatchWriter getDataWriter() {
        return getElementWriter(getDataTableName());
    }

    protected BatchWriter getWriterFromElementType(Element element) {
        if (element instanceof Vertex) {
            return getVerticesWriter();
        } else if (element instanceof Edge) {
            return getEdgesWriter();
        } else {
            throw new VertexiumException("Unexpected element type: " + element.getClass().getName());
        }
    }

    protected BatchWriter getMetadataWriter() {
        try {
            if (this.metadataWriter.get() != null) {
                return this.metadataWriter.get();
            }
            BatchWriterConfig writerConfig = getConfiguration().createBatchWriterConfig();
            this.metadataWriter.set(this.connector.createBatchWriter(getMetadataTableName(), writerConfig));
            return this.metadataWriter.get();
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) throws VertexiumException {
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

            addMutations(getVerticesWriter(), getDeleteRowMutation(vertex.getId()));

            if (hasEventListeners()) {
                queueEvent(new DeleteVertexEvent(this, vertex));
            }
        } finally {
            trace.stop();
        }
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

            addMutations(getVerticesWriter(), getSoftDeleteRowMutation(vertex.getId(), timestamp));

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

            addMutations(getVerticesWriter(), getMarkHiddenRowMutation(vertex.getId(), columnVisibility));

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
            for (Edge edge : vertex.getEdges(Direction.BOTH, FetchHint.ALL_INCLUDING_HIDDEN, authorizations)) {
                markEdgeVisible(edge, visibility, authorizations);
            }

            addMutations(getVerticesWriter(), getMarkVisibleRowMutation(vertex.getId(), columnVisibility));

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

                    AccumuloEdge edge = AccumuloGraph.this.createEdge(AccumuloGraph.this, this, timestampLong, authorizations);
                    return savePreparedEdge(this, edge, null, authorizations);
                } finally {
                    trace.stop();
                }
            }

            @Override
            protected AccumuloEdge createEdge(Authorizations authorizations) {
                return AccumuloGraph.this.createEdge(AccumuloGraph.this, this, timestampLong, authorizations);
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

                    AccumuloEdge edge = createEdge(AccumuloGraph.this, this, timestampLong, authorizations);
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
            Authorizations authorizations
    ) {
        Iterable<Visibility> hiddenVisibilities = null;
        AccumuloEdge edge = new AccumuloEdge(
                accumuloGraph,
                edgeBuilder.getEdgeId(),
                edgeBuilder.getOutVertexId(),
                edgeBuilder.getInVertexId(),
                edgeBuilder.getLabel(),
                edgeBuilder.getNewEdgeLabel(),
                edgeBuilder.getVisibility(),
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                hiddenVisibilities,
                timestamp,
                authorizations
        );
        return edge;
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
        }

        if (hasEventListeners()) {
            queueEvent(new AddEdgeEvent(AccumuloGraph.this, edge));
            for (Property property : edgeBuilder.getProperties()) {
                queueEvent(new AddPropertyEvent(AccumuloGraph.this, edge, property));
            }
            for (PropertyDeleteMutation propertyDeleteMutation : edgeBuilder.getPropertyDeletes()) {
                queueEvent(new DeletePropertyEvent(AccumuloGraph.this, edge, propertyDeleteMutation));
            }
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : edgeBuilder.getPropertySoftDeletes()) {
                queueEvent(new SoftDeletePropertyEvent(AccumuloGraph.this, edge, propertySoftDeleteMutation));
            }
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

            EnumSet<FetchHint> fetchHints = EnumSet.of(FetchHint.PROPERTIES, FetchHint.PROPERTY_METADATA);
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
                        if (value instanceof StreamingPropertyValueTableRef) {
                            value = ((StreamingPropertyValueTableRef) value).toStreamingPropertyValue(this);
                        }
                        String propertyKey = propertyColumnQualifier.getPropertyKey();
                        String propertyName = propertyColumnQualifier.getPropertyName();
                        Visibility propertyVisibility = accumuloVisibilityToVisibility(columnVisibility);
                        HistoricalPropertyValue hpv = new HistoricalPropertyValue(propertyKey, propertyName, propertyVisibility, timestamp, value, metadata, hiddenVisibilities);
                        results.put(resultsKey, hpv);
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
                return new TreeSet<>(results.values());
            } finally {
                scanner.close();
            }
        } finally {
            trace.stop();
        }
    }

    private static abstract class AddEdgeToVertexRunnable {
        public abstract void run(AccumuloEdge edge);
    }

    @Override
    public CloseableIterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
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

            addMutations(getVerticesWriter(), outMutation, inMutation);

            // Deletes everything else related to edge.
            addMutations(getEdgesWriter(), getDeleteRowMutation(edge.getId()));

            if (hasEventListeners()) {
                queueEvent(new DeleteEdgeEvent(this, edge));
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

            addMutations(getVerticesWriter(), outMutation, inMutation);

            // Soft deletes everything else related to edge.
            addMutations(getEdgesWriter(), getSoftDeleteRowMutation(edge.getId(), timestamp));

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

            addMutations(getVerticesWriter(), outMutation, inMutation);

            // Delete everything else related to edge.
            addMutations(getEdgesWriter(), getMarkHiddenRowMutation(edge.getId(), columnVisibility));

            if (out instanceof AccumuloVertex) {
                ((AccumuloVertex) out).removeOutEdge(edge);
            }
            if (in instanceof AccumuloVertex) {
                ((AccumuloVertex) in).removeInEdge(edge);
            }

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
            Vertex out = edge.getVertex(Direction.OUT, FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
            if (out == null) {
                throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
            }
            Vertex in = edge.getVertex(Direction.IN, FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
            if (in == null) {
                throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            Mutation outMutation = new Mutation(out.getId());
            outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility);

            Mutation inMutation = new Mutation(in.getId());
            inMutation.putDelete(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility);

            addMutations(getVerticesWriter(), outMutation, inMutation);

            // Delete everything else related to edge.
            addMutations(getEdgesWriter(), getMarkVisibleRowMutation(edge.getId(), columnVisibility));

            if (out instanceof AccumuloVertex) {
                ((AccumuloVertex) out).addOutEdge(edge);
            }
            if (in instanceof AccumuloVertex) {
                ((AccumuloVertex) in).addInEdge(edge);
            }

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
                addMutations(getVerticesWriter(), getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility));
            } else if (element instanceof Edge) {
                addMutations(getEdgesWriter(), getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility));
            }

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
                addMutations(getVerticesWriter(), getMarkVisiblePropertyMutation(element.getId(), property, timestamp, columnVisibility));
            } else if (element instanceof Edge) {
                addMutations(getEdgesWriter(), getMarkVisiblePropertyMutation(element.getId(), property, timestamp, columnVisibility));
            }

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
        flushWriter(this.elementWriter.get());
        super.flush();
    }

    private void flushGraphEventQueue() {
        GraphEvent graphEvent;
        while ((graphEvent = this.graphEventQueue.poll()) != null) {
            fireGraphEvent(graphEvent);
        }
    }

    private static void flushWriter(VertexiumMultiTableBatchWriter writer) {
        if (writer == null) {
            return;
        }

        writer.flush();
    }

    @Override
    public void shutdown() {
        try {
            flush();
            super.shutdown();
            fileSystem.close();
            this.graphMetadataStore.close();
            this.curatorFramework.close();
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
    public Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) throws VertexiumException {
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
    public Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        Span trace = Trace.start("getVerticesWithPrefix");
        trace.data("vertexIdPrefix", vertexIdPrefix);
        traceDataFetchHints(trace, fetchHints);
        org.apache.accumulo.core.data.Range range = org.apache.accumulo.core.data.Range.prefix(vertexIdPrefix);
        return getVerticesInRange(trace, range, fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(Range idRange, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
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
            EnumSet<FetchHint> fetchHints,
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
            EnumSet<FetchHint> fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(fetchHints, ElementType.VERTEX, maxVersions, startTime, endTime, Lists.newArrayList(range), authorizations);
    }

    protected ScannerBase createEdgeScanner(
            EnumSet<FetchHint> fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(fetchHints, ElementType.EDGE, maxVersions, startTime, endTime, Lists.newArrayList(range), authorizations);
    }

    private ScannerBase createElementScanner(
            EnumSet<FetchHint> fetchHints,
            ElementType elementType,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws VertexiumException {
        return createElementScanner(fetchHints, elementType, maxVersions, startTime, endTime, ranges, true, authorizations);
    }

    private ScannerBase createElementScanner(
            EnumSet<FetchHint> fetchHints,
            ElementType elementType,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            boolean useVertexiumElementIterators,
            Authorizations authorizations
    ) throws VertexiumException {
        try {
            String tableName = getTableNameFromElementType(elementType);
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

    public static EnumSet<org.vertexium.accumulo.iterator.model.FetchHint> toIteratorFetchHints(EnumSet<FetchHint> fetchHints) {
        List<org.vertexium.accumulo.iterator.model.FetchHint> results = new ArrayList<>();
        for (FetchHint fetchHint : fetchHints) {
            results.add(toIteratorFetchHint(fetchHint));
        }
        return org.vertexium.accumulo.iterator.model.FetchHint.create(results);
    }

    private static org.vertexium.accumulo.iterator.model.FetchHint toIteratorFetchHint(FetchHint fetchHint) {
        return org.vertexium.accumulo.iterator.model.FetchHint.valueOf(fetchHint.name());
    }

    protected ScannerBase createVertexScanner(
            EnumSet<FetchHint> fetchHints,
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
            EnumSet<FetchHint> fetchHints,
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

    private ScannerBase createBatchScanner(
            String tableName,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws TableNotFoundException {
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = toAccumuloAuthorizations(authorizations);
        return createBatchScanner(tableName, ranges, accumuloAuthorizations);
    }

    private ScannerBase createBatchScanner(
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

    private void applyFetchHints(ScannerBase scanner, EnumSet<FetchHint> fetchHints, ElementType elementType) {
        scanner.clearColumns();
        if (fetchHints.equals(FetchHint.ALL)) {
            return;
        }

        Iterable<Text> columnFamiliesToFetch = getColumnFamiliesToFetch(elementType, fetchHints);
        for (Text columnFamilyToFetch : columnFamiliesToFetch) {
            scanner.fetchColumnFamily(columnFamilyToFetch);
        }
    }

    public static Iterable<Text> getColumnFamiliesToFetch(ElementType elementType, EnumSet<FetchHint> fetchHints) {
        List<Text> columnFamiliesToFetch = new ArrayList<>();

        columnFamiliesToFetch.add(AccumuloElement.CF_HIDDEN);
        columnFamiliesToFetch.add(AccumuloElement.CF_SOFT_DELETE);

        if (elementType == ElementType.VERTEX) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_SIGNAL);
        } else if (elementType == ElementType.EDGE) {
            columnFamiliesToFetch.add(AccumuloEdge.CF_SIGNAL);
            columnFamiliesToFetch.add(AccumuloEdge.CF_IN_VERTEX);
            columnFamiliesToFetch.add(AccumuloEdge.CF_OUT_VERTEX);
        } else {
            throw new VertexiumException("Unhandled element type: " + elementType);
        }

        if (fetchHints.contains(FetchHint.IN_EDGE_REFS) || fetchHints.contains(FetchHint.IN_EDGE_LABELS)) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE);
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE_HIDDEN);
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE_SOFT_DELETE);
        }
        if (fetchHints.contains(FetchHint.OUT_EDGE_REFS) || fetchHints.contains(FetchHint.OUT_EDGE_LABELS)) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE);
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE_HIDDEN);
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE);
        }
        if (fetchHints.contains(FetchHint.PROPERTIES)) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
        }
        if (fetchHints.contains(FetchHint.PROPERTY_METADATA)) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_METADATA);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
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

    public org.apache.accumulo.core.security.Authorizations toAccumuloAuthorizations(Authorizations authorizations) {
        if (authorizations == null) {
            throw new NullPointerException("authorizations is required");
        }
        return new org.apache.accumulo.core.security.Authorizations(authorizations.getAuthorizations());
    }

    @Override
    public Edge getEdge(String edgeId, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
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

    public byte[] streamingPropertyValueTableData(String dataRowKey) {
        try {
            final long timerStartTime = System.currentTimeMillis();
            org.apache.accumulo.core.data.Range range = new org.apache.accumulo.core.data.Range(dataRowKey);
            Scanner scanner = createScanner(getDataTableName(), range, new org.apache.accumulo.core.security.Authorizations());
            GRAPH_LOGGER.logStartIterator(scanner);
            Span trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKey", dataRowKey);
            try {
                Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
                if (it.hasNext()) {
                    Map.Entry<Key, Value> col = it.next();
                    return col.getValue().get();
                }
            } finally {
                scanner.close();
                trace.stop();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } catch (Exception ex) {
            throw new VertexiumException(ex);
        }
        throw new VertexiumException("Unexpected end of row: " + dataRowKey);
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

    public static String getEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_e");
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

    public String getEdgesTableName() {
        return edgesTableName;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public String getDataDir() {
        return dataDir;
    }

    public Connector getConnector() {
        return connector;
    }

    public Iterable<Range> listVerticesTableSplits() {
        try {
            return splitsIterableToRangeIterable(getConnector().tableOperations().listSplits(getVerticesTableName()));
        } catch (Exception ex) {
            throw new VertexiumException("Could not get splits for: " + getVerticesTableName(), ex);
        }
    }

    public Iterable<Range> listEdgesTableSplits() {
        try {
            return splitsIterableToRangeIterable(getConnector().tableOperations().listSplits(getEdgesTableName()));
        } catch (Exception ex) {
            throw new VertexiumException("Could not get splits for: " + getVerticesTableName(), ex);
        }
    }

    public Iterable<Range> listDataTableSplits() {
        try {
            return splitsIterableToRangeIterable(getConnector().tableOperations().listSplits(getDataTableName()));
        } catch (Exception ex) {
            throw new VertexiumException("Could not get splits for: " + getVerticesTableName(), ex);
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

    void alterElementVisibility(AccumuloElement element, Visibility newVisibility, Authorizations authorizations) {
        BatchWriter elementWriter = getWriterFromElementType(element);
        String elementRowKey = element.getId();
        Visibility oldVisibility = element.getVisibility();
        Span trace = Trace.start("alterElementVisibility");
        trace.data("elementRowKey", elementRowKey);
        try {
            if (element instanceof Edge) {
                BatchWriter vertexWriter = getVerticesWriter();
                Edge edge = (Edge) element;

                String vertexOutRowKey = edge.getVertexId(Direction.OUT);
                Mutation vertexOutMutation = new Mutation(vertexOutRowKey);
                if (elementMutationBuilder.alterEdgeVertexOutVertex(vertexOutMutation, edge, newVisibility)) {
                    addMutations(vertexWriter, vertexOutMutation);
                }

                String vertexInRowKey = edge.getVertexId(Direction.IN);
                Mutation vertexInMutation = new Mutation(vertexInRowKey);
                if (elementMutationBuilder.alterEdgeVertexInVertex(vertexInMutation, edge, newVisibility)) {
                    addMutations(vertexWriter, vertexInMutation);
                }
            }

            Mutation m = new Mutation(elementRowKey);
            if (elementMutationBuilder.alterElementVisibility(m, element, newVisibility)) {
                addMutations(elementWriter, m);
            }
            element.setVisibility(newVisibility);
            getSearchIndex().alterElementVisibility(
                    AccumuloGraph.this,
                    element,
                    oldVisibility,
                    newVisibility,
                    authorizations
            );
        } finally {
            trace.stop();
        }
    }

    public void alterEdgeLabel(AccumuloEdge edge, String newEdgeLabel) {
        elementMutationBuilder.alterEdgeLabel(edge, newEdgeLabel);
    }

    void alterElementPropertyVisibilities(
            AccumuloElement element,
            List<AlterPropertyVisibility> alterPropertyVisibilities,
            Authorizations authorizations
    ) {
        if (alterPropertyVisibilities.size() == 0) {
            return;
        }

        BatchWriter writer = getWriterFromElementType(element);
        String elementRowKey = element.getId();

        boolean propertyChanged = false;
        Mutation m = new Mutation(elementRowKey);
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
            property.setTimestamp(IncreasingTime.currentTimeMillis());
            elementMutationBuilder.addPropertyToMutation(this, m, elementRowKey, property);

            // delete the property with the old/existing visibility from the search index
            getSearchIndex().deleteProperty(
                    this,
                    element,
                    apv.getKey(),
                    apv.getName(),
                    apv.getExistingVisibility(),
                    authorizations
            );

            propertyChanged = true;
        }
        if (propertyChanged) {
            addMutations(writer, m);
        }
    }

    void alterPropertyMetadatas(AccumuloElement element, List<SetPropertyMetadata> setPropertyMetadatas) {
        if (setPropertyMetadatas.size() == 0) {
            return;
        }

        List<Property> propertiesToSave = new ArrayList<>();
        for (SetPropertyMetadata apm : setPropertyMetadatas) {
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new VertexiumException(String.format("Could not find property %s:%s(%s)", apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility()));
            }
            property.getMetadata().add(apm.getMetadataName(), apm.getNewValue(), apm.getMetadataVisibility());
            propertiesToSave.add(property);
        }

        BatchWriter writer = getWriterFromElementType(element);
        String elementRowKey = element.getId();

        Mutation m = new Mutation(elementRowKey);
        for (Property property : propertiesToSave) {
            elementMutationBuilder.addPropertyMetadataToMutation(m, property);
        }
        addMutations(writer, m);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }

    @Override
    public void truncate() {
        try {
            this.connector.tableOperations().deleteRows(getDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getEdgesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getVerticesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getMetadataTableName(), null, null);
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
            ScannerBase scanner = createElementScanner(
                    EnumSet.of(FetchHint.OUT_EDGE_REFS),
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
            ScannerBase scanner = createElementScanner(
                    EnumSet.of(FetchHint.OUT_EDGE_REFS),
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
                            for (Iterator<RelatedEdge> i = results.iterator(); i.hasNext(); ) {
                                if (softDeletedEdgeIds.contains(i.next().getEdgeId())) {
                                    i.remove();
                                }
                            }
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
    public Iterable<Path> findPaths(String sourceVertexId, String destVertexId, String[] labels, int maxHops, ProgressCallback progressCallback, Authorizations authorizations) {
        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        List<Path> foundPaths = new ArrayList<>();
        if (maxHops < 1) {
            throw new IllegalArgumentException("maxHops cannot be less than 1");
        } else if (maxHops == 1) {
            Set<String> sourceConnectedVertexIds = getConnectedVertexIds(sourceVertexId, labels, authorizations);
            if (sourceConnectedVertexIds.contains(destVertexId)) {
                foundPaths.add(new Path(sourceVertexId, destVertexId));
            }
        } else if (maxHops == 2) {
            findPathsSetIntersection(foundPaths, sourceVertexId, destVertexId, labels, progressCallback, authorizations);
        } else {
            findPathsBreadthFirst(foundPaths, sourceVertexId, destVertexId, labels, maxHops, progressCallback, authorizations);
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths;
    }

    protected void findPathsSetIntersection(List<Path> foundPaths, String sourceVertexId, String destVertexId, String[] labels, ProgressCallback progressCallback, Authorizations authorizations) {
        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(sourceVertexId);
        vertexIds.add(destVertexId);
        Map<String, Set<String>> connectedVertexIds = getConnectedVertexIds(vertexIds, labels, authorizations);

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = connectedVertexIds.get(sourceVertexId);
        if (sourceVertexConnectedVertexIds == null) {
            return;
        }

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = connectedVertexIds.get(destVertexId);
        if (destVertexConnectedVertexIds == null) {
            return;
        }

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        for (String connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private void findPathsBreadthFirst(List<Path> foundPaths, String sourceVertexId, String destVertexId, String[] labels, int hops, ProgressCallback progressCallback, Authorizations authorizations) {
        Map<String, Set<String>> connectedVertexIds = getConnectedVertexIds(sourceVertexId, destVertexId, labels, authorizations);
        // start at 2 since we already got the source and dest vertex connected vertex ids
        for (int i = 2; i < hops; i++) {
            progressCallback.progress((double) i / (double) hops, ProgressCallback.Step.FINDING_PATH);
            Set<String> vertexIdsToSearch = new HashSet<>();
            for (Map.Entry<String, Set<String>> entry : connectedVertexIds.entrySet()) {
                vertexIdsToSearch.addAll(entry.getValue());
            }
            vertexIdsToSearch.removeAll(connectedVertexIds.keySet());
            Map<String, Set<String>> r = getConnectedVertexIds(vertexIdsToSearch, labels, authorizations);
            connectedVertexIds.putAll(r);
        }
        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        Set<String> seenVertices = new HashSet<>();
        Path currentPath = new Path(sourceVertexId);
        findPathsRecursive(connectedVertexIds, foundPaths, sourceVertexId, destVertexId, labels, hops, seenVertices, currentPath, progressCallback);
    }

    protected void findPathsRecursive(
            Map<String, Set<String>> connectedVertexIds,
            List<Path> foundPaths,
            final String sourceVertexId,
            String destVertexId,
            String[] labels,
            int hops,
            Set<String> seenVertices,
            Path currentPath,
            @SuppressWarnings("UnusedParameters") ProgressCallback progressCallback
    ) {
        seenVertices.add(sourceVertexId);
        if (sourceVertexId.equals(destVertexId)) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Set<String> vertexIds = connectedVertexIds.get(sourceVertexId);
            if (vertexIds != null) {
                for (String childId : vertexIds) {
                    if (!seenVertices.contains(childId)) {
                        findPathsRecursive(connectedVertexIds, foundPaths, childId, destVertexId, labels, hops - 1, seenVertices, new Path(currentPath, childId), progressCallback);
                    }
                }
            }
        }
        seenVertices.remove(sourceVertexId);
    }

    private Set<String> getConnectedVertexIds(String vertexId, String[] labels, Authorizations authorizations) {
        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(vertexId);
        Map<String, Set<String>> results = getConnectedVertexIds(vertexIds, labels, authorizations);
        Set<String> vertexIdResults = results.get(vertexId);
        if (vertexIdResults == null) {
            return new HashSet<>();
        }
        return vertexIdResults;
    }

    private Map<String, Set<String>> getConnectedVertexIds(String vertexId1, String vertexId2, String[] labels, Authorizations authorizations) {
        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(vertexId1);
        vertexIds.add(vertexId2);
        return getConnectedVertexIds(vertexIds, labels, authorizations);
    }

    private Map<String, Set<String>> getConnectedVertexIds(Set<String> vertexIds, String[] labels, Authorizations authorizations) {
        Span trace = Trace.start("getConnectedVertexIds");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("getConnectedVertexIds:\n  %s", IterableUtils.join(vertexIds, "\n  "));
            }

            if (vertexIds.size() == 0) {
                return new HashMap<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIds) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            int maxVersions = 1;
            Long startTime = null;
            Long endTime = null;
            ScannerBase scanner = createElementScanner(
                    FetchHint.EDGE_REFS,
                    ElementType.VERTEX,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting connectedVertexIdsIteratorSettings = new IteratorSetting(
                    1000,
                    ConnectedVertexIdsIterator.class.getSimpleName(),
                    ConnectedVertexIdsIterator.class
            );
            ConnectedVertexIdsIterator.setLabels(connectedVertexIdsIteratorSettings, labels);
            scanner.addScanIterator(connectedVertexIdsIteratorSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Map<String, Set<String>> results = new HashMap<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    try {
                        Set<String> rowVertexIds = ConnectedVertexIdsIterator.decodeValue(row.getValue());
                        results.put(row.getKey().getRow().toString(), rowVertexIds);
                    } catch (IOException e) {
                        throw new VertexiumException("Could not decode vertex ids for row: " + row.getKey().toString(), e);
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
                    FetchHint.ALL_INCLUDING_HIDDEN,
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

    protected CloseableIterable<Vertex> getVerticesInRange(
            final Span trace,
            final org.apache.accumulo.core.data.Range range,
            final EnumSet<FetchHint> fetchHints,
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
                return createVertexFromVertexIteratorValue(next.getKey(), next.getValue(), authorizations);
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

    private Vertex createVertexFromVertexIteratorValue(Key key, Value value, Authorizations authorizations) {
        return AccumuloVertex.createFromIteratorValue(this, key, value, authorizations);
    }

    private Edge createEdgeFromEdgeIteratorValue(Key key, Value value, Authorizations authorizations) {
        return AccumuloEdge.createFromIteratorValue(this, key, value, authorizations);
    }

    @Override
    public CloseableIterable<Vertex> getVertices(Iterable<String> ids, final EnumSet<FetchHint> fetchHints, final Long endTime, final Authorizations authorizations) {
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
                return createVertexFromVertexIteratorValue(row.getKey(), row.getValue(), authorizations);
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
    public CloseableIterable<Edge> getEdges(Iterable<String> ids, final EnumSet<FetchHint> fetchHints, final Long endTime, final Authorizations authorizations) {
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
                return createEdgeFromEdgeIteratorValue(row.getKey(), row.getValue(), authorizations);
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
    public Iterable<Edge> getEdgesInRange(Range idRange, EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
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
            final EnumSet<FetchHint> fetchHints,
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
            final Span trace,
            final org.apache.accumulo.core.data.Range range,
            final EnumSet<FetchHint> fetchHints,
            final Long endTime,
            final Authorizations authorizations
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
                return createEdgeFromEdgeIteratorValue(next.getKey(), next.getValue(), authorizations);
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
        traceOn(description, new HashMap<String, String>());
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

    private void traceDataFetchHints(Span trace, EnumSet<FetchHint> fetchHints) {
        if (Trace.isTracing()) {
            trace.data("fetchHints", FetchHint.toString(fetchHints));
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
            this.treeCache.getListenable().addListener(new TreeCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("treeCache event, clearing cache");
                    }
                    synchronized (entries) {
                        entries.clear();
                    }
                }
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
                writer.flush();
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

    private class VertexiumMultiTableBatchWriter {
        private final MultiTableBatchWriter multiTableBatchWriter;

        public VertexiumMultiTableBatchWriter(MultiTableBatchWriter multiTableBatchWriter) {
            this.multiTableBatchWriter = multiTableBatchWriter;
        }

        public BatchWriter getBatchWriter(String tableName) {
            try {
                return multiTableBatchWriter.getBatchWriter(tableName);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException ex) {
                throw new VertexiumException("Could not get batch writer for table: " + tableName, ex);
            }
        }

        public void flush() {
            try {
                multiTableBatchWriter.flush();
            } catch (MutationsRejectedException ex) {
                throw new VertexiumException("Could not flush writer", ex);
            }
        }

        @Override
        public String toString() {
            return "VertexiumMultiTableBatchWriter{" +
                    "multiTableBatchWriter=" + multiTableBatchWriter +
                    '}';
        }

        @Override
        protected void finalize() throws Throwable {
            if (!multiTableBatchWriter.isClosed()) {
                multiTableBatchWriter.close();
            }
            super.finalize();
        }
    }
}
