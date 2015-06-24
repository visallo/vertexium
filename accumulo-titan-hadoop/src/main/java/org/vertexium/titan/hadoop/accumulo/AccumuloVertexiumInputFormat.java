package org.vertexium.titan.hadoop.accumulo;

import com.thinkaurelius.titan.graphdb.schema.VertexLabelDefinition;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexLabel;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.vertexium.*;
import org.vertexium.accumulo.AccumuloAuthorizations;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.VertexMaker;
import org.vertexium.accumulo.mapreduce.VertexiumMRUtils;
import org.vertexium.util.MapUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AccumuloVertexiumInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {
    private final AccumuloRowInputFormat accumuloInputFormat;
    private Configuration config;
    private VertexQueryFilter vertexQuery;

    public AccumuloVertexiumInputFormat() {
        this.accumuloInputFormat = new AccumuloRowInputFormat();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return this.accumuloInputFormat.getSplits(context);
    }

    public static void configure(Job job) {
        try {
            AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(job.getConfiguration(), "graph.");
            AccumuloGraph graph = AccumuloGraph.create(accumuloGraphConfiguration);
            String principal = accumuloGraphConfiguration.getAccumuloUsername();
            AuthenticationToken token = accumuloGraphConfiguration.getAuthenticationToken();
            String instanceName = accumuloGraphConfiguration.getAccumuloInstanceName();
            String zooKeepers = accumuloGraphConfiguration.getZookeeperServers();
            String[] authorizations = job.getConfiguration().get("titan.hadoop.input.authorizations", "").split(",");
            if (authorizations.length == 1 && authorizations[0].trim().length() == 0) {
                authorizations = new String[0];
            }

            String tableName = graph.getVerticesTableName();
            AccumuloRowInputFormat.setInputTableName(job, tableName);
            AccumuloRowInputFormat.setConnectorInfo(job, principal, token);
            ClientConfiguration clientConfig = new ClientConfiguration()
                    .withInstance(instanceName)
                    .withZkHosts(zooKeepers);
            AccumuloRowInputFormat.setZooKeeperInstance(job, clientConfig);
            AccumuloRowInputFormat.setScanAuthorizations(job, new org.apache.accumulo.core.security.Authorizations(authorizations));
            job.getConfiguration().setStrings(VertexiumMRUtils.CONFIG_AUTHORIZATIONS, authorizations);
        } catch (Exception ex) {
            throw new VertexiumException("Could not configure", ex);
        }
    }

    @Override
    public void setConf(Configuration config) {
        this.config = config;
        this.vertexQuery = VertexQueryFilter.create(config);
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final RecordReader<Text, PeekingIterator<Map.Entry<Key, Value>>> reader = accumuloInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        return new RecordReader<NullWritable, FaunusVertex>() {
            public FaunusVertex vertex;
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
                if (!reader.nextKeyValue()) {
                    return false;
                }

                PeekingIterator<Map.Entry<Key, Value>> row = reader.getCurrentValue();
                vertex = createFaunusVertexFromRow(graph, row, authorizations);
                vertexQuery.defaultFilter(vertex);
                return true;
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
                return NullWritable.get();
            }

            @Override
            public FaunusVertex getCurrentValue() throws IOException, InterruptedException {
                return vertex;
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

    private FaunusVertex createFaunusVertexFromRow(AccumuloGraph graph, PeekingIterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        VertexMaker maker = new VertexMaker(graph, row, authorizations);
        final Vertex v = maker.make(false);
        final long vertexId = toFaunusVertexId(v.getId());
        FaunusVertex faunusVertex = new FaunusVertex();
        faunusVertex.setId(vertexId);
        faunusVertex.setVertexLabel(v.getId());
        for (Property property : v.getProperties()) {
            faunusVertex.addProperty(property.getName(), property.getValue());
        }
        for (EdgeInfo edgeInfo : v.getEdgeInfos(Direction.OUT, authorizations)) {
            faunusVertex.addEdge(com.tinkerpop.blueprints.Direction.OUT, edgeInfo.getLabel(), toFaunusVertexId(edgeInfo.getVertexId()));
        }
        for (EdgeInfo edgeInfo : v.getEdgeInfos(Direction.IN, authorizations)) {
            faunusVertex.addEdge(com.tinkerpop.blueprints.Direction.IN, edgeInfo.getLabel(), toFaunusVertexId(edgeInfo.getVertexId()));
        }
        return faunusVertex;
    }

    private long toFaunusVertexId(String id) {
        return Math.abs(id.hashCode());
    }
}
