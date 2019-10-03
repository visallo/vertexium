package org.vertexium.accumulo.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.vertexium.*;
import org.vertexium.accumulo.AccumuloAuthorizations;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.LazyMutableProperty;
import org.vertexium.accumulo.iterator.model.ElementData;
import org.vertexium.accumulo.iterator.model.IteratorFetchHints;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;
import org.vertexium.util.MapUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

@Deprecated
public abstract class AccumuloElementInputFormatBase<TValue extends Element> extends InputFormat<Text, TValue> {
    private final AccumuloRowInputFormat accumuloInputFormat;

    public AccumuloElementInputFormatBase() {
        accumuloInputFormat = new AccumuloRowInputFormat();
    }

    public static void setInputInfo(Job job, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations, String tableName) throws AccumuloSecurityException {
        AccumuloRowInputFormat.setInputTableName(job, tableName);
        AccumuloRowInputFormat.setConnectorInfo(job, principal, token);
        ClientConfiguration clientConfig = ClientConfiguration.create()
            .withInstance(instanceName)
            .withZkHosts(zooKeepers);
        AccumuloRowInputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloRowInputFormat.setScanAuthorizations(job, new org.apache.accumulo.core.security.Authorizations(authorizations));
        job.getConfiguration().setStrings(VertexiumMRUtils.CONFIG_AUTHORIZATIONS, authorizations);
    }

    public static void setFetchHints(Job job, ElementType elementType, FetchHints fetchHints) {
        Iterable<Text> columnFamiliesToFetch = AccumuloGraph.getColumnFamiliesToFetch(elementType, fetchHints);
        Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new ArrayList<>();
        for (Text columnFamilyToFetch : columnFamiliesToFetch) {
            columnFamilyColumnQualifierPairs.add(new Pair<>(columnFamilyToFetch, null));
        }
        AccumuloInputFormat.fetchColumns(job, columnFamilyColumnQualifierPairs);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        return accumuloInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<Text, TValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final RecordReader<Text, PeekingIterator<Map.Entry<Key, Value>>> reader = accumuloInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        return new RecordReader<Text, TValue>() {
            public AccumuloGraph graph;
            public Authorizations authorizations;

            @Override
            @SuppressWarnings("unchecked")
            public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException {
                reader.initialize(inputSplit, ctx);

                Map configurationMap = VertexiumMRUtils.toMap(ctx.getConfiguration());
                this.graph = (AccumuloGraph) new GraphFactory().createGraph(MapUtils.getAllWithPrefix(configurationMap, "graph"));
                this.authorizations = new AccumuloAuthorizations(ctx.getConfiguration().getStrings(VertexiumMRUtils.CONFIG_AUTHORIZATIONS));
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return reader.nextKeyValue();
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return reader.getCurrentKey();
            }

            @Override
            public TValue getCurrentValue() throws IOException, InterruptedException {
                PeekingIterator<Map.Entry<Key, Value>> row = reader.getCurrentValue();
                return createElementFromRow(graph, row, authorizations);
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return reader.getProgress();
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    protected abstract TValue createElementFromRow(AccumuloGraph graph, PeekingIterator<Map.Entry<Key, Value>> row, Authorizations authorizations);

    protected static Iterable<Property> makePropertiesFromElementData(final AccumuloGraph graph, ElementData elementData, IteratorFetchHints fetchHints) {
        return Iterables.transform(elementData.getProperties(fetchHints), new Function<org.vertexium.accumulo.iterator.model.Property, Property>() {
            @Nullable
            @Override
            public Property apply(@Nullable org.vertexium.accumulo.iterator.model.Property property) {
                return makePropertyFromIteratorProperty(graph, property);
            }
        });
    }

    private static Property makePropertyFromIteratorProperty(AccumuloGraph graph, org.vertexium.accumulo.iterator.model.Property property) {
        Set<Visibility> hiddenVisibilities = null;
        if (property.hiddenVisibilities != null) {
            hiddenVisibilities = Sets.newHashSet(Iterables.transform(property.hiddenVisibilities, new Function<ByteSequence, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(ByteSequence visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(ByteSequenceUtils.toString(visibilityText)));
                }
            }));
        }
        Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(
            AccumuloGraph.visibilityToAccumuloVisibility(property.visibility)
        );
        return new LazyMutableProperty(
            graph,
            graph.getVertexiumSerializer(),
            graph.getNameSubstitutionStrategy().inflate(property.key),
            graph.getNameSubstitutionStrategy().inflate(property.name),
            property.value,
            null,
            hiddenVisibilities,
            visibility,
            property.timestamp,
            FetchHints.ALL_INCLUDING_HIDDEN
        );
    }
}
