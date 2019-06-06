package org.vertexium.accumulo.mapreduce;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.vertexium.*;
import org.vertexium.accumulo.*;
import org.vertexium.accumulo.util.StreamingPropertyValueStorageStrategy;
import org.vertexium.id.IdGenerator;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.util.IncreasingTime;

import java.io.IOException;

public abstract class ElementMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public static final String GRAPH_CONFIG_PREFIX = "graphConfigPrefix";
    private ElementMutationBuilder elementMutationBuilder;
    private ElementMapperGraph graph;
    private NameSubstitutionStrategy nameSubstitutionStrategy;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        String configPrefix = context.getConfiguration().get(GRAPH_CONFIG_PREFIX, "");
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(context.getConfiguration(), configPrefix);
        String tableNamePrefix = accumuloGraphConfiguration.getTableNamePrefix();
        final Text edgesTableName = new Text(AccumuloGraph.getEdgesTableName(tableNamePrefix));
        final Text dataTableName = new Text(AccumuloGraph.getDataTableName(tableNamePrefix));
        final Text verticesTableName = new Text(AccumuloGraph.getVerticesTableName(tableNamePrefix));
        final Text extendedDataTableName = new Text(AccumuloGraph.getExtendedDataTableName(tableNamePrefix));

        this.graph = new ElementMapperGraph(this);
        VertexiumSerializer vertexiumSerializer = accumuloGraphConfiguration.createSerializer(this.graph);
        nameSubstitutionStrategy = accumuloGraphConfiguration.createSubstitutionStrategy(this.graph);
        StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy = accumuloGraphConfiguration.createStreamingPropertyValueStorageStrategy(this.graph);
        this.elementMutationBuilder = new ElementMutationBuilder(streamingPropertyValueStorageStrategy, vertexiumSerializer) {
            @Override
            protected void saveVertexMutation(Mutation m) {
                try {
                    ElementMapper.this.saveVertexMutation(context, verticesTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save vertex", e);
                }
            }

            @Override
            protected void saveEdgeMutation(Mutation m) {
                try {
                    ElementMapper.this.saveEdgeMutation(context, edgesTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save edge", e);
                }
            }

            @Override
            protected void saveExtendedDataMutation(ElementType elementType, Mutation m) {
                try {
                    ElementMapper.this.saveExtendedDataMutation(context, extendedDataTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save edge", e);
                }
            }

            @Override
            protected NameSubstitutionStrategy getNameSubstitutionStrategy() {
                return nameSubstitutionStrategy;
            }

            @Override
            public void saveDataMutation(Mutation m) {
                try {
                    ElementMapper.this.saveDataMutation(context, dataTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save data", e);
                }
            }
        };
    }

    protected abstract void saveDataMutation(Context context, Text dataTableName, Mutation m) throws IOException, InterruptedException;

    protected abstract void saveEdgeMutation(Context context, Text edgesTableName, Mutation m) throws IOException, InterruptedException;

    protected abstract void saveVertexMutation(Context context, Text verticesTableName, Mutation m) throws IOException, InterruptedException;

    protected abstract void saveExtendedDataMutation(Context context, Text tableName, Mutation m) throws IOException, InterruptedException;

    public VertexBuilder prepareVertex(Vertex vertex) {
        return prepareVertex(vertex.getId(), null, vertex.getVisibility());
    }

    public VertexBuilder prepareVertex(String vertexId, Visibility visibility) {
        return prepareVertex(vertexId, null, visibility);
    }

    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                AccumuloGraph graph = null;
                Iterable<Visibility> hiddenVisibilities = null;

                // This has to occur before createVertex since it will mutate the properties
                elementMutationBuilder.saveVertexBuilder(graph, this, timestampLong);

                return new AccumuloVertex(
                    graph,
                    getId(),
                    getVisibility(),
                    getProperties(),
                    getPropertyDeletes(),
                    getPropertySoftDeletes(),
                    hiddenVisibilities,
                    getAdditionalVisibilitiesAsStringSet(),
                    getExtendedDataTableNames(),
                    timestampLong,
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    authorizations
                );
            }
        };
    }

    public Edge addEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility).save(authorizations);
    }

    public EdgeBuilderByVertexId prepareEdge(Edge edge) {
        return prepareEdge(
            edge.getId(),
            edge.getVertexId(Direction.OUT),
            edge.getVertexId(Direction.IN),
            edge.getLabel(),
            edge.getTimestamp(),
            edge.getVisibility()
        );
    }

    public EdgeBuilderByVertexId prepareEdge(
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Visibility visibility
    ) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, null, visibility);
    }

    public EdgeBuilderByVertexId prepareEdge(
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Long timestamp,
        Visibility visibility
    ) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                // This has to occur before createEdge since it will mutate the properties
                elementMutationBuilder.saveEdgeBuilder(null, this, timestampLong);

                AccumuloEdge edge = new AccumuloEdge(
                    null,
                    getId(),
                    getVertexId(Direction.OUT),
                    getVertexId(Direction.IN),
                    getEdgeLabel(),
                    getNewEdgeLabel(),
                    getVisibility(),
                    getProperties(),
                    getPropertyDeletes(),
                    getPropertySoftDeletes(),
                    null,
                    getAdditionalVisibilitiesAsStringSet(),
                    getExtendedDataTableNames(),
                    timestampLong,
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    authorizations
                );
                return edge;
            }
        };
    }

    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility);
    }

    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                // This has to occur before createEdge since it will mutate the properties
                elementMutationBuilder.saveEdgeBuilder(null, this, timestampLong);

                AccumuloEdge edge = new AccumuloEdge(
                    null,
                    getId(),
                    getOutVertex().getId(),
                    getInVertex().getId(),
                    getEdgeLabel(),
                    getNewEdgeLabel(),
                    getVisibility(),
                    getProperties(),
                    getPropertyDeletes(),
                    getPropertySoftDeletes(),
                    null,
                    getAdditionalVisibilitiesAsStringSet(),
                    getExtendedDataTableNames(),
                    timestampLong,
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    authorizations
                );
                return edge;
            }
        };
    }

    public IdGenerator getIdGenerator() {
        throw new VertexiumException("not implemented");
    }

    public Graph getGraph() {
        return graph;
    }
}
