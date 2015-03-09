package org.neolumin.vertexium.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.neolumin.vertexium.*;
import org.neolumin.vertexium.accumulo.iterator.ElementVisibilityRowFilter;
import org.neolumin.vertexium.accumulo.serializer.ValueSerializer;
import org.neolumin.vertexium.event.*;
import org.neolumin.vertexium.id.IdGenerator;
import org.neolumin.vertexium.mutation.AlterPropertyVisibility;
import org.neolumin.vertexium.mutation.PropertyRemoveMutation;
import org.neolumin.vertexium.mutation.SetPropertyMetadata;
import org.neolumin.vertexium.property.MutableProperty;
import org.neolumin.vertexium.property.StreamingPropertyValue;
import org.neolumin.vertexium.search.IndexHint;
import org.neolumin.vertexium.search.SearchIndex;
import org.neolumin.vertexium.util.CloseableIterable;
import org.neolumin.vertexium.util.EmptyClosableIterable;
import org.neolumin.vertexium.util.JavaSerializableUtils;
import org.neolumin.vertexium.util.LookAheadIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.neolumin.vertexium.util.IterableUtils.singleOrDefault;
import static org.neolumin.vertexium.util.IterableUtils.toSet;
import static org.neolumin.vertexium.util.Preconditions.checkNotNull;

public class AccumuloGraph extends GraphBaseWithSearchIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloGraph.class);
    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    public static final Text DELETE_ROW_COLUMN_FAMILY = new Text("");
    public static final Text DELETE_ROW_COLUMN_QUALIFIER = new Text("");
    public static final Text METADATA_COLUMN_FAMILY = new Text("");
    public static final Text METADATA_COLUMN_QUALIFIER = new Text("");
    public static final String VERTEX_AFTER_ROW_KEY_PREFIX = "W";
    public static final String EDGE_AFTER_ROW_KEY_PREFIX = "F";
    private static final Object addIteratorLock = new Object();
    private static final Integer METADATA_ACCUMULO_GRAPH_VERSION = 2;
    private static final String METADATA_ACCUMULO_GRAPH_VERSION_KEY = "accumulo.graph.version";
    private static final String METADATA_VALUE_SERIALIZER = "accumulo.graph.valueSerializer";
    private static final Authorizations METADATA_AUTHORIZATIONS = new AccumuloAuthorizations();
    private final Connector connector;
    private final ValueSerializer valueSerializer;
    private final FileSystem fileSystem;
    private final String dataDir;
    private BatchWriter verticesWriter;
    private BatchWriter edgesWriter;
    private BatchWriter dataWriter;
    private BatchWriter metadataWriter;
    protected ElementMutationBuilder elementMutationBuilder;
    private final Queue<GraphEvent> graphEventQueue = new LinkedList<>();
    private Integer accumuloGraphVersion;
    private boolean foundValueSerializerMetadata;

    protected AccumuloGraph(AccumuloGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex, Connector connector, FileSystem fileSystem, ValueSerializer valueSerializer) {
        super(config, idGenerator, searchIndex);
        this.connector = connector;
        this.valueSerializer = valueSerializer;
        this.fileSystem = fileSystem;
        this.dataDir = config.getDataDir();
        long maxStreamingPropertyValueTableDataSize = config.getMaxStreamingPropertyValueTableDataSize();
        this.elementMutationBuilder = new ElementMutationBuilder(fileSystem, valueSerializer, maxStreamingPropertyValueTableDataSize, dataDir) {
            @Override
            protected void saveVertexMutation(Mutation m) {
                addMutations(getVerticesWriter(), m);
            }

            @Override
            protected void saveEdgeMutation(Mutation m) {
                addMutations(getEdgesWriter(), m);
            }

            @Override
            protected void saveDataMutation(Mutation dataMutation) {
                addMutations(getDataWriter(), dataMutation);
            }

            @Override
            protected StreamingPropertyValueRef saveStreamingPropertyValue(String rowKey, Property property, StreamingPropertyValue propertyValue) {
                StreamingPropertyValueRef streamingPropertyValueRef = super.saveStreamingPropertyValue(rowKey, property, propertyValue);
                ((MutableProperty) property).setValue(streamingPropertyValueRef.toStreamingPropertyValue(AccumuloGraph.this));
                return streamingPropertyValueRef;
            }
        };
    }

    public static AccumuloGraph create(AccumuloGraphConfiguration config) throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        Connector connector = config.createConnector();
        FileSystem fs = config.createFileSystem();
        ValueSerializer valueSerializer = config.createValueSerializer();
        SearchIndex searchIndex = config.createSearchIndex();
        IdGenerator idGenerator = config.createIdGenerator();
        ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getDataTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getMetadataTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getDataTableName(config.getTableNamePrefix()));
        AccumuloGraph graph = new AccumuloGraph(config, idGenerator, searchIndex, connector, fs, valueSerializer);
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
        foundValueSerializerMetadata = false;
        super.setupGraphMetadata();
        if (!foundValueSerializerMetadata) {
            setMetadata(METADATA_VALUE_SERIALIZER, valueSerializer.getClass().getName());
        }
    }

    @Override
    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        super.setupGraphMetadata(graphMetadataEntry);
        if (graphMetadataEntry.getKey().equals(METADATA_ACCUMULO_GRAPH_VERSION_KEY)) {
            if (graphMetadataEntry.getValue() instanceof Integer) {
                accumuloGraphVersion = (Integer) graphMetadataEntry.getValue();
                LOGGER.info(METADATA_ACCUMULO_GRAPH_VERSION_KEY + "=" + accumuloGraphVersion);
            } else {
                throw new VertexiumException("Invalid accumulo version in metadata. " + graphMetadataEntry);
            }
        } else if (graphMetadataEntry.getKey().equals(METADATA_VALUE_SERIALIZER)) {
            if (graphMetadataEntry.getValue() instanceof String) {
                String valueSerializerClassName = (String) graphMetadataEntry.getValue();
                if (!valueSerializerClassName.equals(valueSerializer.getClass().getName())) {
                    throw new VertexiumException("Invalid " + METADATA_VALUE_SERIALIZER + " expected " + valueSerializerClassName + " found " + valueSerializer.getClass().getName());
                }
                foundValueSerializerMetadata = true;
            } else {
                throw new VertexiumException("Invalid " + METADATA_VALUE_SERIALIZER + " expected string found " + graphMetadataEntry.getValue().getClass().getName());
            }
        }
    }

    protected static void ensureTableExists(Connector connector, String tableName) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
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
                        LOGGER.warn("Failed to attach RowDeletingIterator. Retrying in " + SLEEP_TIME + "ms.");
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

    public static AccumuloGraph create(Map config) throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        return create(new AccumuloGraphConfiguration(config));
    }

    @Override
    public Vertex addVertex(String vertexId, Visibility vertexVisibility, Authorizations authorizations) {
        return prepareVertex(vertexId, vertexVisibility).save(authorizations);
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                AccumuloVertex vertex = new AccumuloVertex(
                        AccumuloGraph.this,
                        getVertexId(),
                        getVisibility(),
                        getProperties(),
                        getPropertyRemoves(),
                        null,
                        authorizations,
                        System.currentTimeMillis()
                );

                elementMutationBuilder.saveVertex(vertex);

                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(AccumuloGraph.this, vertex, authorizations);
                }

                if (hasEventListeners()) {
                    queueEvent(new AddVertexEvent(AccumuloGraph.this, vertex));
                    for (Property property : getProperties()) {
                        queueEvent(new AddPropertyEvent(AccumuloGraph.this, vertex, property));
                    }
                    for (PropertyRemoveMutation propertyRemoveMutation : getPropertyRemoves()) {
                        queueEvent(new RemovePropertyEvent(AccumuloGraph.this, vertex, propertyRemoveMutation));
                    }
                }

                return vertex;
            }
        };
    }

    private void queueEvent(GraphEvent graphEvent) {
        synchronized (this.graphEventQueue) {
            this.graphEventQueue.add(graphEvent);
        }
    }

    void saveProperties(AccumuloElement element, Iterable<Property> properties, Iterable<PropertyRemoveMutation> propertyRemoves, IndexHint indexHint, Authorizations authorizations) {
        String rowPrefix = getRowPrefixForElement(element);

        String elementRowKey = rowPrefix + element.getId();
        Mutation m = new Mutation(elementRowKey);
        boolean hasProperty = false;
        for (PropertyRemoveMutation propertyRemove : propertyRemoves) {
            hasProperty = true;
            elementMutationBuilder.addPropertyRemoveToMutation(m, propertyRemove);
        }
        for (Property property : properties) {
            hasProperty = true;
            elementMutationBuilder.addPropertyToMutation(m, elementRowKey, property);
        }
        if (hasProperty) {
            addMutations(getWriterFromElementType(element), m);
        }

        if (indexHint != IndexHint.DO_NOT_INDEX) {
            for (PropertyRemoveMutation propertyRemoveMutation : propertyRemoves) {
                getSearchIndex().removeProperty(
                        this,
                        element,
                        propertyRemoveMutation.getKey(),
                        propertyRemoveMutation.getName(),
                        propertyRemoveMutation.getVisibility(),
                        authorizations
                );
            }
            getSearchIndex().addElement(this, element, authorizations);
        }

        if (hasEventListeners()) {
            for (Property property : properties) {
                queueEvent(new AddPropertyEvent(AccumuloGraph.this, element, property));
            }
            for (PropertyRemoveMutation propertyRemoveMutation : propertyRemoves) {
                queueEvent(new RemovePropertyEvent(AccumuloGraph.this, element, propertyRemoveMutation));
            }
        }
    }

    void removeProperty(AccumuloElement element, Property property, Authorizations authorizations) {
        String rowPrefix = getRowPrefixForElement(element);

        Mutation m = new Mutation(rowPrefix + element.getId());
        elementMutationBuilder.addPropertyRemoveToMutation(m, property);
        addMutations(getWriterFromElementType(element), m);

        getSearchIndex().removeProperty(this, element, property, authorizations);

        if (hasEventListeners()) {
            queueEvent(new RemovePropertyEvent(this, element, property));
        }
    }

    private String getRowPrefixForElement(AccumuloElement element) {
        if (element instanceof Vertex) {
            return AccumuloConstants.VERTEX_ROW_KEY_PREFIX;
        }
        if (element instanceof Edge) {
            return AccumuloConstants.EDGE_ROW_KEY_PREFIX;
        }
        throw new VertexiumException("Unexpected element type: " + element.getClass().getName());
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

    protected BatchWriter getVerticesWriter() {
        try {
            // to avoid a synchronized block check verticesWriter first and return it.
            if (this.verticesWriter != null) {
                return this.verticesWriter;
            }
            synchronized (this) {
                if (this.verticesWriter != null) {
                    return this.verticesWriter;
                }
                BatchWriterConfig writerConfig = new BatchWriterConfig();
                this.verticesWriter = this.connector.createBatchWriter(getVerticesTableName(), writerConfig);
                return this.verticesWriter;
            }
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    protected BatchWriter getEdgesWriter() {
        try {
            // to avoid a synchronized block check edgesWriter first and return it.
            if (this.edgesWriter != null) {
                return this.edgesWriter;
            }
            synchronized (this) {
                if (this.edgesWriter != null) {
                    return this.edgesWriter;
                }
                BatchWriterConfig writerConfig = new BatchWriterConfig();
                this.edgesWriter = this.connector.createBatchWriter(getEdgesTableName(), writerConfig);
                return this.edgesWriter;
            }
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
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

    protected BatchWriter getDataWriter() {
        try {
            // to avoid a synchronized block check dataWriter first and return it.
            if (this.dataWriter != null) {
                return this.dataWriter;
            }
            synchronized (this) {
                if (this.dataWriter != null) {
                    return this.dataWriter;
                }
                BatchWriterConfig writerConfig = new BatchWriterConfig();
                this.dataWriter = this.connector.createBatchWriter(getDataTableName(), writerConfig);
                return this.dataWriter;
            }
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    protected BatchWriter getMetadataWriter() {
        try {
            // to avoid a synchronized block check dataWriter first and return it.
            if (this.metadataWriter != null) {
                return this.metadataWriter;
            }
            synchronized (this) {
                if (this.metadataWriter != null) {
                    return this.metadataWriter;
                }
                BatchWriterConfig writerConfig = new BatchWriterConfig();
                this.metadataWriter = this.connector.createBatchWriter(getMetadataTableName(), writerConfig);
                return this.metadataWriter;
            }
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Authorizations authorizations) throws VertexiumException {
        return getVerticesInRange(null, null, fetchHints, authorizations);
    }

    @Override
    public void removeVertex(Vertex vertex, Authorizations authorizations) {
        if (vertex == null) {
            throw new IllegalArgumentException("vertex cannot be null");
        }

        getSearchIndex().removeElement(this, vertex, authorizations);

        // Remove all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            removeEdge(edge, authorizations);
        }

        addMutations(getVerticesWriter(), getDeleteRowMutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertex.getId()));

        if (hasEventListeners()) {
            queueEvent(new RemoveVertexEvent(this, vertex));
        }
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");

        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

        // Remove all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            markEdgeHidden(edge, visibility, authorizations);
        }

        addMutations(getVerticesWriter(), getMarkHiddenRowMutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertex.getId(), columnVisibility));

        if (hasEventListeners()) {
            queueEvent(new MarkHiddenVertexEvent(this, vertex));
        }
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");

        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

        // Remove all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, FetchHint.ALL_INCLUDING_HIDDEN, authorizations)) {
            markEdgeVisible(edge, visibility, authorizations);
        }

        addMutations(getVerticesWriter(), getMarkVisibleRowMutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertex.getId(), columnVisibility));

        if (hasEventListeners()) {
            queueEvent(new MarkVisibleVertexEvent(this, vertex));
        }
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                return savePreparedEdge(this, getOutVertexId(), getInVertexId(), null, authorizations);
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        if (outVertex == null) {
            throw new IllegalArgumentException("outVertex is required");
        }
        if (inVertex == null) {
            throw new IllegalArgumentException("inVertex is required");
        }
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
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
                return savePreparedEdge(this, getOutVertex().getId(), getInVertex().getId(), addEdgeToVertex, authorizations);
            }
        };
    }

    private Edge savePreparedEdge(EdgeBuilderBase edgeBuilder, String outVertexId, String inVertexId, AddEdgeToVertexRunnable addEdgeToVertex, Authorizations authorizations) {
        AccumuloEdge edge = new AccumuloEdge(
                AccumuloGraph.this,
                edgeBuilder.getEdgeId(),
                outVertexId,
                inVertexId,
                edgeBuilder.getLabel(),
                edgeBuilder.getNewEdgeLabel(),
                edgeBuilder.getVisibility(),
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyRemoves(),
                null,
                authorizations,
                System.currentTimeMillis()
        );
        elementMutationBuilder.saveEdge(edge);

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
            for (PropertyRemoveMutation propertyRemoveMutation : edgeBuilder.getPropertyRemoves()) {
                queueEvent(new RemovePropertyEvent(AccumuloGraph.this, edge, propertyRemoveMutation));
            }
        }

        return edge;
    }

    private static abstract class AddEdgeToVertexRunnable {
        public abstract void run(AccumuloEdge edge);
    }

    @Override
    public CloseableIterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getEdgesInRange(null, null, fetchHints, authorizations);
    }

    @Override
    public void removeEdge(Edge edge, Authorizations authorizations) {
        checkNotNull(edge);

        getSearchIndex().removeElement(this, edge, authorizations);

        ColumnVisibility visibility = visibilityToAccumuloVisibility(edge.getVisibility());

        Mutation outMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.OUT));
        outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), visibility);

        Mutation inMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.IN));
        inMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), visibility);

        addMutations(getVerticesWriter(), outMutation, inMutation);

        // Remove everything else related to edge.
        addMutations(getEdgesWriter(), getDeleteRowMutation(AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId()));

        if (hasEventListeners()) {
            queueEvent(new RemoveEdgeEvent(this, edge));
        }
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        checkNotNull(edge);

        Vertex out = edge.getVertex(Direction.OUT, authorizations);
        if (out == null) {
            throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
        }
        Vertex in = edge.getVertex(Direction.IN, authorizations);
        if (in == null) {
            throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
        }

        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

        Mutation outMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + out.getId());
        outMutation.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, AccumuloElement.HIDDEN_VALUE);

        Mutation inMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + in.getId());
        inMutation.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility, AccumuloElement.HIDDEN_VALUE);

        addMutations(getVerticesWriter(), outMutation, inMutation);

        // Remove everything else related to edge.
        addMutations(getEdgesWriter(), getMarkHiddenRowMutation(AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId(), columnVisibility));

        if (out instanceof AccumuloVertex) {
            ((AccumuloVertex) out).removeOutEdge(edge);
        }
        if (in instanceof AccumuloVertex) {
            ((AccumuloVertex) in).removeInEdge(edge);
        }

        if (hasEventListeners()) {
            queueEvent(new MarkHiddenEdgeEvent(this, edge));
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        checkNotNull(edge);

        Vertex out = edge.getVertex(Direction.OUT, FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
        if (out == null) {
            throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
        }
        Vertex in = edge.getVertex(Direction.IN, FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
        if (in == null) {
            throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
        }

        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

        Mutation outMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + out.getId());
        outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility);

        Mutation inMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + in.getId());
        inMutation.putDelete(AccumuloVertex.CF_IN_EDGE_HIDDEN, new Text(edge.getId()), columnVisibility);

        addMutations(getVerticesWriter(), outMutation, inMutation);

        // Remove everything else related to edge.
        addMutations(getEdgesWriter(), getMarkVisibleRowMutation(AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId(), columnVisibility));

        if (out instanceof AccumuloVertex) {
            ((AccumuloVertex) out).addOutEdge(edge);
        }
        if (in instanceof AccumuloVertex) {
            ((AccumuloVertex) in).addInEdge(edge);
        }

        if (hasEventListeners()) {
            queueEvent(new MarkVisibleEdgeEvent(this, edge));
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }

    @SuppressWarnings("unused")
    public void markPropertyHidden(AccumuloElement element, Property property, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element);

        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

        if (element instanceof Vertex) {
            addMutations(getVerticesWriter(), getMarkHiddenPropertyMutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + element.getId(), property, columnVisibility));
        } else if (element instanceof Edge) {
            addMutations(getVerticesWriter(), getMarkHiddenPropertyMutation(AccumuloConstants.EDGE_ROW_KEY_PREFIX + element.getId(), property, columnVisibility));
        }

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenPropertyEvent(this, element, property, visibility));
        }
    }

    private Mutation getMarkHiddenPropertyMutation(String rowKey, Property property, ColumnVisibility visibility) {
        Mutation m = new Mutation(rowKey);
        Text columnQualifier = ElementMutationBuilder.getPropertyColumnQualifierWithVisibilityString(property);
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, AccumuloElement.HIDDEN_VALUE);
        return m;
    }

    @SuppressWarnings("unused")
    public void markPropertyVisible(AccumuloElement element, Property property, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element);

        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

        if (element instanceof Vertex) {
            addMutations(getVerticesWriter(), getMarkVisiblePropertyMutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + element.getId(), property, columnVisibility));
        } else if (element instanceof Edge) {
            addMutations(getVerticesWriter(), getMarkVisiblePropertyMutation(AccumuloConstants.EDGE_ROW_KEY_PREFIX + element.getId(), property, columnVisibility));
        }

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisiblePropertyEvent(this, element, property, visibility));
        }
    }

    private Mutation getMarkVisiblePropertyMutation(String rowKey, Property property, ColumnVisibility visibility) {
        Mutation m = new Mutation(rowKey);
        Text columnQualifier = ElementMutationBuilder.getPropertyColumnQualifierWithVisibilityString(property);
        m.putDelete(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility);
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
        flushWriter(this.dataWriter);
        flushWriter(this.verticesWriter);
        flushWriter(this.edgesWriter);
        super.flush();
    }

    private void flushGraphEventQueue() {
        GraphEvent graphEvent;
        while ((graphEvent = this.graphEventQueue.poll()) != null) {
            fireGraphEvent(graphEvent);
        }
    }

    private static void flushWriter(BatchWriter writer) {
        if (writer != null) {
            try {
                writer.flush();
            } catch (MutationsRejectedException e) {
                throw new VertexiumException("Could not flush", e);
            }
        }
    }

    @Override
    public void shutdown() {
        try {
            flush();
            if (this.dataWriter != null) {
                this.dataWriter.close();
                this.dataWriter = null;
            }
            if (this.verticesWriter != null) {
                this.verticesWriter.close();
                this.verticesWriter = null;
            }
            if (this.edgesWriter != null) {
                this.edgesWriter.close();
                this.edgesWriter = null;
            }
            super.shutdown();
        } catch (Exception ex) {
            throw new VertexiumException(ex);
        }
    }

    private Mutation getDeleteRowMutation(String rowKey) {
        Mutation m = new Mutation(rowKey);
        m.put(DELETE_ROW_COLUMN_FAMILY, DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
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

    public ValueSerializer getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public AccumuloGraphConfiguration getConfiguration() {
        return (AccumuloGraphConfiguration) super.getConfiguration();
    }

    @Override
    public Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) throws VertexiumException {
        Iterator<Vertex> vertices = getVerticesInRange(new Range(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertexId), fetchHints, authorizations).iterator();
        if (vertices.hasNext()) {
            return vertices.next();
        }
        return null;
    }

    @Override
    public CloseableIterable<Vertex> getVertices(Iterable<String> ids, final EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        final AccumuloGraph graph = this;
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        final List<Range> ranges = new ArrayList<>();
        for (String id : ids) {
            Text rowKey = new Text(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + id);
            ranges.add(new Range(rowKey));
        }
        if (ranges.size() == 0) {
            return new EmptyClosableIterable<>();
        }

        return new LookAheadIterable<Map.Entry<Key, Value>, Vertex>() {
            public BatchScanner batchScanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Map.Entry<Key, Value> wholeRow) {
                try {
                    SortedMap<Key, Value> row = WholeRowIterator.decodeRow(wholeRow.getKey(), wholeRow.getValue());
                    VertexMaker maker = new VertexMaker(graph, row.entrySet().iterator(), authorizations);
                    return maker.make(includeHidden);
                } catch (IOException ex) {
                    throw new VertexiumException("Could not recreate row", ex);
                }
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                batchScanner = createVertexBatchScanner(fetchHints, authorizations, Math.min(Math.max(1, ranges.size() / 10), 10));
                batchScanner.setRanges(ranges);
                return batchScanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                batchScanner.close();
            }
        };
    }

    private CloseableIterable<Vertex> getVerticesInRange(String startId, String endId, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) throws VertexiumException {
        final Key startKey;
        if (startId == null) {
            startKey = new Key(AccumuloConstants.VERTEX_ROW_KEY_PREFIX);
        } else {
            startKey = new Key(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = new Key(VERTEX_AFTER_ROW_KEY_PREFIX);
        } else {
            endKey = new Key(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + endId + "~");
        }

        Range range = new Range(startKey, endKey);
        return getVerticesInRange(range, fetchHints, authorizations);
    }

    protected CloseableIterable<Vertex> getVerticesInRange(final Range range, final EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        return new LookAheadIterable<Iterator<Map.Entry<Key, Value>>, Vertex>() {
            public Scanner scanner;

            @Override
            protected boolean isIncluded(Iterator<Map.Entry<Key, Value>> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Iterator<Map.Entry<Key, Value>> next) {
                VertexMaker maker = new VertexMaker(AccumuloGraph.this, next, authorizations);
                return maker.make(includeHidden);
            }

            @Override
            protected Iterator<Iterator<Map.Entry<Key, Value>>> createIterator() {
                scanner = createVertexScanner(fetchHints, authorizations);
                scanner.setRange(range);
                return new RowIterator(scanner.iterator());
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
            }
        };
    }

    protected Scanner createVertexScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations) throws VertexiumException {
        return createElementVisibilityScanner(fetchHints, authorizations, ElementType.VERTEX);
    }

    protected Scanner createEdgeScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations) throws VertexiumException {
        return createElementVisibilityScanner(fetchHints, authorizations, ElementType.EDGE);
    }

    private Scanner createElementVisibilityScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations, ElementType elementType) throws VertexiumException {
        try {
            String tableName = getTableNameFromElementType(elementType);
            Scanner scanner = connector.createScanner(tableName, toAccumuloAuthorizations(authorizations));
            if (getConfiguration().isUseServerSideElementVisibilityRowFilter()) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                        100,
                        ElementVisibilityRowFilter.class.getSimpleName(),
                        ElementVisibilityRowFilter.class
                );
                String elementMode = getElementModeFromElementType(elementType);
                iteratorSetting.addOption(elementMode, Boolean.TRUE.toString());
                scanner.addScanIterator(iteratorSetting);
            }
            applyFetchHints(scanner, fetchHints, elementType);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new VertexiumException(e);
        }
    }

    protected BatchScanner createVertexBatchScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations, int numQueryThreads) throws VertexiumException {
        return createElementVisibilityWholeRowBatchScanner(fetchHints, authorizations, ElementType.VERTEX, numQueryThreads);
    }

    protected BatchScanner createEdgeBatchScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations, int numQueryThreads) throws VertexiumException {
        return createElementVisibilityWholeRowBatchScanner(fetchHints, authorizations, ElementType.EDGE, numQueryThreads);
    }

    private BatchScanner createElementVisibilityWholeRowBatchScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations, ElementType elementType, int numQueryThreads) throws VertexiumException {
        BatchScanner scanner = createElementVisibilityBatchScanner(fetchHints, authorizations, elementType, numQueryThreads);
        IteratorSetting iteratorSetting;

        iteratorSetting = new IteratorSetting(
                101,
                WholeRowIterator.class.getSimpleName(),
                WholeRowIterator.class
        );
        scanner.addScanIterator(iteratorSetting);

        return scanner;
    }

    private BatchScanner createElementVisibilityBatchScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations, ElementType elementType, int numQueryThreads) {
        BatchScanner scanner = createElementBatchScanner(fetchHints, authorizations, elementType, numQueryThreads);
        IteratorSetting iteratorSetting;
        if (getConfiguration().isUseServerSideElementVisibilityRowFilter()) {
            iteratorSetting = new IteratorSetting(
                    100,
                    ElementVisibilityRowFilter.class.getSimpleName(),
                    ElementVisibilityRowFilter.class
            );
            String elementMode = getElementModeFromElementType(elementType);
            iteratorSetting.addOption(elementMode, Boolean.TRUE.toString());
            scanner.addScanIterator(iteratorSetting);
        }
        return scanner;
    }

    private BatchScanner createElementBatchScanner(EnumSet<FetchHint> fetchHints, Authorizations authorizations, ElementType elementType, int numQueryThreads) {
        try {
            String tableName = getTableNameFromElementType(elementType);
            BatchScanner scanner = connector.createBatchScanner(tableName, toAccumuloAuthorizations(authorizations), numQueryThreads);
            applyFetchHints(scanner, fetchHints, elementType);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new VertexiumException(e);
        }
    }

    private void applyFetchHints(ScannerBase scanner, EnumSet<FetchHint> fetchHints, ElementType elementType) {
        scanner.clearColumns();
        if (fetchHints.equals(FetchHint.ALL)) {
            return;
        }

        scanner.fetchColumnFamily(AccumuloElement.CF_HIDDEN);

        if (elementType == ElementType.VERTEX) {
            scanner.fetchColumnFamily(AccumuloVertex.CF_SIGNAL);
        } else if (elementType == ElementType.EDGE) {
            scanner.fetchColumnFamily(AccumuloEdge.CF_SIGNAL);
            scanner.fetchColumnFamily(AccumuloEdge.CF_IN_VERTEX);
            scanner.fetchColumnFamily(AccumuloEdge.CF_OUT_VERTEX);
        } else {
            throw new VertexiumException("Unhandled element type: " + elementType);
        }

        if (fetchHints.contains(FetchHint.IN_EDGE_REFS)) {
            scanner.fetchColumnFamily(AccumuloVertex.CF_IN_EDGE);
            scanner.fetchColumnFamily(AccumuloVertex.CF_IN_EDGE_HIDDEN);
        }
        if (fetchHints.contains(FetchHint.OUT_EDGE_REFS)) {
            scanner.fetchColumnFamily(AccumuloVertex.CF_OUT_EDGE);
            scanner.fetchColumnFamily(AccumuloVertex.CF_OUT_EDGE_HIDDEN);
        }
        if (fetchHints.contains(FetchHint.PROPERTIES)) {
            scanner.fetchColumnFamily(AccumuloElement.CF_PROPERTY_HIDDEN);
            scanner.fetchColumnFamily(AccumuloElement.CF_PROPERTY);
        }
        if (fetchHints.contains(FetchHint.PROPERTY_METADATA)) {
            scanner.fetchColumnFamily(AccumuloElement.CF_PROPERTY_HIDDEN);
            scanner.fetchColumnFamily(AccumuloElement.CF_PROPERTY_METADATA);
        }
    }

    private String getTableNameFromElementType(ElementType elementType) {
        String tableName;
        switch (elementType) {
            case VERTEX:
                tableName = getVerticesTableName();
                break;
            case EDGE:
                tableName = getEdgesTableName();
                break;
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
        return tableName;
    }

    private String getElementModeFromElementType(ElementType elementType) {
        String elementMode;
        switch (elementType) {
            case VERTEX:
                elementMode = ElementVisibilityRowFilter.OPT_FILTER_VERTICES;
                break;
            case EDGE:
                elementMode = ElementVisibilityRowFilter.OPT_FILTER_EDGES;
                break;
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
        return elementMode;
    }

    private org.apache.accumulo.core.security.Authorizations toAccumuloAuthorizations(Authorizations authorizations) {
        if (authorizations == null) {
            throw new NullPointerException("authorizations is required");
        }
        return new org.apache.accumulo.core.security.Authorizations(authorizations.getAuthorizations());
    }

    @Override
    public Edge getEdge(String edgeId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        Iterator<Edge> edges = getEdgesInRange(edgeId, edgeId, fetchHints, authorizations).iterator();
        if (edges.hasNext()) {
            return edges.next();
        }
        return null;
    }

    @Override
    public CloseableIterable<Edge> getEdges(Iterable<String> ids, final EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        final AccumuloGraph graph = this;
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        final List<Range> ranges = new ArrayList<>();
        for (String id : ids) {
            Text rowKey = new Text(AccumuloConstants.EDGE_ROW_KEY_PREFIX + id);
            ranges.add(new Range(rowKey));
        }
        if (ranges.size() == 0) {
            return new EmptyClosableIterable<>();
        }

        return new LookAheadIterable<Map.Entry<Key, Value>, Edge>() {
            public BatchScanner batchScanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Map.Entry<Key, Value> wholeRow) {
                try {
                    SortedMap<Key, Value> row = WholeRowIterator.decodeRow(wholeRow.getKey(), wholeRow.getValue());
                    EdgeMaker maker = new EdgeMaker(graph, row.entrySet().iterator(), authorizations);
                    return maker.make(includeHidden);
                } catch (IOException ex) {
                    throw new VertexiumException("Could not recreate row", ex);
                }
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                batchScanner = createEdgeBatchScanner(fetchHints, authorizations, Math.min(Math.max(1, ranges.size() / 10), 10));
                batchScanner.setRanges(ranges);
                return batchScanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                batchScanner.close();
            }
        };
    }

    protected CloseableIterable<Edge> getEdgesInRange(String startId, String endId, final EnumSet<FetchHint> fetchHints, final Authorizations authorizations) throws VertexiumException {
        final AccumuloGraph graph = this;
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        final Key startKey;
        if (startId == null) {
            startKey = new Key(AccumuloConstants.EDGE_ROW_KEY_PREFIX);
        } else {
            startKey = new Key(AccumuloConstants.EDGE_ROW_KEY_PREFIX + startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = new Key(EDGE_AFTER_ROW_KEY_PREFIX);
        } else {
            endKey = new Key(AccumuloConstants.EDGE_ROW_KEY_PREFIX + endId + "~");
        }

        return new LookAheadIterable<Iterator<Map.Entry<Key, Value>>, Edge>() {
            public Scanner scanner;

            @Override
            protected boolean isIncluded(Iterator<Map.Entry<Key, Value>> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Iterator<Map.Entry<Key, Value>> next) {
                EdgeMaker maker = new EdgeMaker(graph, next, authorizations);
                return maker.make(includeHidden);
            }

            @Override
            protected Iterator<Iterator<Map.Entry<Key, Value>>> createIterator() {
                scanner = createEdgeScanner(fetchHints, authorizations);
                scanner.setRange(new Range(startKey, endKey));
                return new RowIterator(scanner.iterator());
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
            }
        };
    }

    @SuppressWarnings("unused")
    private void printTable(Authorizations authorizations) {
        String[] tables = new String[]{getEdgesTableName(), getVerticesTableName(), getDataTableName()};
        System.out.println("---------------------------------------------- BEGIN printTable ----------------------------------------------");
        try {
            for (String tableName : tables) {
                System.out.println("TABLE: " + tableName);
                System.out.println("");
                Scanner scanner = connector.createScanner(tableName, toAccumuloAuthorizations(authorizations));
                RowIterator it = new RowIterator(scanner.iterator());
                while (it.hasNext()) {
                    boolean first = true;
                    Text lastColumnFamily = null;
                    Iterator<Map.Entry<Key, Value>> row = it.next();
                    while (row.hasNext()) {
                        Map.Entry<Key, Value> col = row.next();
                        if (first) {
                            System.out.println("\"" + col.getKey().getRow() + "\"");
                            first = false;
                        }
                        if (!col.getKey().getColumnFamily().equals(lastColumnFamily)) {
                            System.out.println("  \"" + col.getKey().getColumnFamily() + "\"");
                            lastColumnFamily = col.getKey().getColumnFamily();
                        }
                        System.out.println("    \"" + col.getKey().getColumnQualifier() + "\"(" + col.getKey().getColumnVisibility() + ")=\"" + col.getValue() + "\"");
                    }
                }
            }
            System.out.flush();
        } catch (TableNotFoundException e) {
            throw new VertexiumException(e);
        }
        System.out.println("---------------------------------------------- END printTable ------------------------------------------------");
    }

    public byte[] streamingPropertyValueTableData(String dataRowKey) {
        try {
            Scanner scanner = connector.createScanner(getDataTableName(), new org.apache.accumulo.core.security.Authorizations());
            scanner.setRange(new Range(dataRowKey));
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            if (it.hasNext()) {
                Map.Entry<Key, Value> col = it.next();
                return col.getValue().get();
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
        String columnVisibilityString = columnVisibility.toString();
        return accumuloVisibilityToVisibility(columnVisibilityString);
    }

    public static Visibility accumuloVisibilityToVisibility(String columnVisibilityString) {
        if (columnVisibilityString.startsWith("[") && columnVisibilityString.endsWith("]")) {
            return new Visibility(columnVisibilityString.substring(1, columnVisibilityString.length() - 1));
        }
        return new Visibility(columnVisibilityString);
    }

    public static String getVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_v";
    }

    public static String getEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_e";
    }

    public static String getDataTableName(String tableNamePrefix) {
        return tableNamePrefix + "_d";
    }

    public static String getMetadataTableName(String tableNamePrefix) {
        return tableNamePrefix + "_m";
    }

    public String getVerticesTableName() {
        return getVerticesTableName(getConfiguration().getTableNamePrefix());
    }

    public String getEdgesTableName() {
        return getEdgesTableName(getConfiguration().getTableNamePrefix());
    }

    public String getDataTableName() {
        return getDataTableName(getConfiguration().getTableNamePrefix());
    }

    public String getMetadataTableName() {
        return getMetadataTableName(getConfiguration().getTableNamePrefix());
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

    void alterElementVisibility(AccumuloElement element, Visibility newVisibility) {
        BatchWriter elementWriter = getWriterFromElementType(element);
        String rowPrefix = getRowPrefixForElement(element);
        String elementRowKey = rowPrefix + element.getId();

        if (element instanceof Edge) {
            BatchWriter vertexWriter = getVerticesWriter();
            Edge edge = (Edge) element;

            String voutRowKey = AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.OUT);
            Mutation mvout = new Mutation(voutRowKey);
            if (elementMutationBuilder.alterEdgeVertexOutVertex(mvout, edge, newVisibility)) {
                addMutations(vertexWriter, mvout);
            }

            String vinRowKey = AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.IN);
            Mutation mvin = new Mutation(vinRowKey);
            if (elementMutationBuilder.alterEdgeVertexInVertex(mvin, edge, newVisibility)) {
                addMutations(vertexWriter, mvin);
            }
        }

        Mutation m = new Mutation(elementRowKey);
        if (elementMutationBuilder.alterElementVisibility(m, element, newVisibility)) {
            addMutations(elementWriter, m);
        }
    }

    public void alterEdgeLabel(AccumuloEdge edge, String newEdgeLabel) {
        elementMutationBuilder.alterEdgeLabel(edge, newEdgeLabel);
    }

    void alterElementPropertyVisibilities(AccumuloElement element, List<AlterPropertyVisibility> alterPropertyVisibilities) {
        if (alterPropertyVisibilities.size() == 0) {
            return;
        }

        BatchWriter writer = getWriterFromElementType(element);
        String rowPrefix = getRowPrefixForElement(element);
        String elementRowKey = rowPrefix + element.getId();

        boolean propertyChanged = false;
        Mutation m = new Mutation(elementRowKey);
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            MutableProperty property = (MutableProperty) element.getProperty(apv.getKey(), apv.getName(), apv.getExistingVisibility());
            if (property == null) {
                throw new VertexiumException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (property.getVisibility().equals(apv.getVisibility())) {
                continue;
            }
            elementMutationBuilder.addPropertyRemoveToMutation(m, property);
            property.setVisibility(apv.getVisibility());
            elementMutationBuilder.addPropertyToMutation(m, elementRowKey, property);
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
        String rowPrefix = getRowPrefixForElement(element);
        String elementRowKey = rowPrefix + element.getId();

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
    public void clearData() {
        try {
            this.connector.tableOperations().deleteRows(getDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getEdgesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getVerticesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getMetadataTableName(), null, null);
            getSearchIndex().clearData();
        } catch (Exception ex) {
            throw new VertexiumException("Could not delete rows", ex);
        }
    }

    @Override
    public Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Authorizations authorizations) {
        Set<String> vertexIdsSet = toSet(vertexIds);

        if (vertexIdsSet.size() == 0) {
            return new HashSet<>();
        }

        List<Range> ranges = new ArrayList<>();
        for (String vertexId : vertexIdsSet) {
            Text rowKey = new Text(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertexId);
            Range range = new Range(rowKey);
            ranges.add(range);
        }

        int numQueryThreads = Math.min(Math.max(1, ranges.size() / 10), 10);
        // only fetch one size of the edge since we are scanning all vertices the edge will appear on the out on one of the vertices
        BatchScanner batchScanner = createElementBatchScanner(EnumSet.of(FetchHint.OUT_EDGE_REFS), authorizations, ElementType.VERTEX, numQueryThreads);
        try {
            batchScanner.setRanges(ranges);

            Iterator<Map.Entry<Key, Value>> it = batchScanner.iterator();
            Set<String> edgeIds = new HashSet<>();
            while (it.hasNext()) {
                Map.Entry<Key, Value> c = it.next();
                if (!c.getKey().getColumnFamily().equals(AccumuloVertex.CF_OUT_EDGE)) {
                    continue;
                }
                EdgeInfo edgeInfo = EdgeInfo.parse(c.getValue());
                if (vertexIdsSet.contains(edgeInfo.getVertexId())) {
                    String edgeId = c.getKey().getColumnQualifier().toString();
                    edgeIds.add(edgeId);
                }
            }
            return edgeIds;
        } finally {
            batchScanner.close();
        }
    }

    public Iterable<GraphMetadataEntry> getMetadataInRange(final Range range) {
        return new LookAheadIterable<Map.Entry<Key, Value>, GraphMetadataEntry>() {
            public BatchScanner batchScanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, GraphMetadataEntry graphMetadataEntry) {
                return true;
            }

            @Override
            protected GraphMetadataEntry convert(Map.Entry<Key, Value> entry) {
                String key = entry.getKey().getRow().toString();
                Object value = JavaSerializableUtils.bytesToObject(entry.getValue().get());
                return new GraphMetadataEntry(key, value);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    batchScanner = connector.createBatchScanner(getMetadataTableName(), toAccumuloAuthorizations(METADATA_AUTHORIZATIONS), 1);
                    Collection<Range> ranges = new ArrayList<>();
                    if (range == null) {
                        ranges.add(new Range());
                    } else {
                        ranges.add(range);
                    }
                    batchScanner.setRanges(ranges);
                } catch (TableNotFoundException ex) {
                    throw new VertexiumException("Could not create metadata scanner", ex);
                }
                return batchScanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                this.batchScanner.close();
            }
        };
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        return getMetadataInRange(null);
    }

    @Override
    public void setMetadata(String key, Object value) {
        try {
            Mutation m = new Mutation(key);
            byte[] valueBytes = JavaSerializableUtils.objectToBytes(value);
            m.put(METADATA_COLUMN_FAMILY, METADATA_COLUMN_QUALIFIER, new Value(valueBytes));
            BatchWriter writer = getMetadataWriter();
            writer.addMutation(m);
            writer.flush();
        } catch (MutationsRejectedException ex) {
            throw new VertexiumException("Could not add metadata " + key, ex);
        }
    }

    @Override
    public Object getMetadata(String key) {
        Range range = new Range(key);
        GraphMetadataEntry entry = singleOrDefault(getMetadataInRange(range), null);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }
}
