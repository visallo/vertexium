package org.vertexium.accumulo.mapreduce;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.vertexium.Authorizations;
import org.vertexium.Element;
import org.vertexium.GraphFactory;
import org.vertexium.accumulo.AccumuloAuthorizations;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.util.MapUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class AccumuloElementInputFormatBase<TValue extends Element> extends InputFormat<Text, TValue> {
    private final AccumuloRowInputFormat accumuloInputFormat;

    public AccumuloElementInputFormatBase() {
        accumuloInputFormat = new AccumuloRowInputFormat();
    }

    protected static void setInputInfo(Job job, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations, String tableName) throws AccumuloSecurityException {
        AccumuloRowInputFormat.setInputTableName(job, tableName);
        AccumuloRowInputFormat.setConnectorInfo(job, principal, token);
        AccumuloRowInputFormat.setZooKeeperInstance(job, instanceName, zooKeepers);
        AccumuloRowInputFormat.setScanAuthorizations(job, new org.apache.accumulo.core.security.Authorizations(authorizations));
        job.getConfiguration().setStrings(VertexiumMRUtils.CONFIG_AUTHORIZATIONS, authorizations);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return accumuloInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<Text, TValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final RecordReader<Text, PeekingIterator<Map.Entry<Key, Value>>> reader = accumuloInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        return new RecordReader<Text, TValue>() {
            public AccumuloGraph graph;
            public Authorizations authorizations;

            @Override
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
}
