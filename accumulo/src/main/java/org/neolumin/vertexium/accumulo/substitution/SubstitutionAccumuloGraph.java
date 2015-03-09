package org.neolumin.vertexium.accumulo.substitution;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.neolumin.vertexium.*;
import org.neolumin.vertexium.accumulo.*;
import org.neolumin.vertexium.accumulo.serializer.ValueSerializer;
import org.neolumin.vertexium.id.IdGenerator;
import org.neolumin.vertexium.property.MutableProperty;
import org.neolumin.vertexium.property.StreamingPropertyValue;
import org.neolumin.vertexium.search.SearchIndex;
import org.neolumin.vertexium.util.CloseableIterable;
import org.neolumin.vertexium.util.EmptyClosableIterable;
import org.neolumin.vertexium.util.LookAheadIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.neolumin.vertexium.util.Preconditions.checkNotNull;

public class SubstitutionAccumuloGraph extends AccumuloGraph {
    private final SubstitutionTemplate substitutionTemplate;

    protected SubstitutionAccumuloGraph(AccumuloGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex, Connector connector, FileSystem fileSystem, ValueSerializer valueSerializer, List<Pair<String, String>> substitutionList) {
        super(config, idGenerator, searchIndex, connector, fileSystem, valueSerializer);
        this.substitutionTemplate = new SimpleSubstitutionTemplate(substitutionList);
        this.elementMutationBuilder = new ElementMutationBuilder(fileSystem, valueSerializer, config.getMaxStreamingPropertyValueTableDataSize(), config.getDataDir()) {
            @Override
            protected Text getPropertyColumnQualifier(Property p){
                return getValueSeparatedJoined(substitutionTemplate.deflate(p.getName()), substitutionTemplate.deflate(p.getKey()));
            }

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
                ((MutableProperty) property).setValue(streamingPropertyValueRef.toStreamingPropertyValue(SubstitutionAccumuloGraph.this));
                return streamingPropertyValueRef;
            }
        };
    }

    public static SubstitutionAccumuloGraph create(SubstitutionAccumuloGraphConfiguration config) throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        Connector connector = config.createConnector();
        FileSystem fs = config.createFileSystem();
        ValueSerializer valueSerializer = config.createValueSerializer();
        SearchIndex searchIndex = config.createSearchIndex();
        IdGenerator idGenerator = config.createIdGenerator();
        List<Pair<String, String>> substitutionList = config.getSubstitionList();

        ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getDataTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getMetadataTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getDataTableName(config.getTableNamePrefix()));
        SubstitutionAccumuloGraph graph = new SubstitutionAccumuloGraph(config, idGenerator, searchIndex, connector, fs, valueSerializer, substitutionList);
        graph.setup();
        return graph;
    }

    public static SubstitutionAccumuloGraph create(Map config) throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        return create(new SubstitutionAccumuloGraphConfiguration(config));
    }

    @Override
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
                VertexMaker maker = new SubstitutionTemplateVertexMaker(SubstitutionAccumuloGraph.this, next, authorizations, substitutionTemplate);
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

    @Override
    public CloseableIterable<Vertex> getVertices(Iterable<String> ids, final EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
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
                    VertexMaker maker = new SubstitutionTemplateVertexMaker(SubstitutionAccumuloGraph.this, row.entrySet().iterator(), authorizations, substitutionTemplate);
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
                    EdgeMaker maker = new SubstitiutionTemplateEdgeMaker(graph, row.entrySet().iterator(), authorizations, substitutionTemplate);
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
                EdgeMaker maker = new SubstitiutionTemplateEdgeMaker(graph, next, authorizations, substitutionTemplate);
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
}
