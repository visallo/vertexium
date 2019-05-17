package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.vertexium.Direction;
import org.vertexium.EdgeBuilderByVertexId;
import org.vertexium.User;
import org.vertexium.Visibility;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.vertexium.accumulo.AccumuloGraph.visibilityToAccumuloVisibility;
import static org.vertexium.accumulo.ElementMutationBuilder.EMPTY_VALUE;

public abstract class AccumuloEdgeBuilderByVertexId extends EdgeBuilderByVertexId implements EdgeBuilderWithKeyValuePairs {
    private final AccumuloGraph graph;
    private final Long timestamp;
    private final ElementMutationBuilder elementMutationBuilder;

    protected AccumuloEdgeBuilderByVertexId(
        AccumuloGraph graph,
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Visibility visibility,
        Long timestamp,
        ElementMutationBuilder elementMutationBuilder
    ) {
        super(edgeId, outVertexId, inVertexId, label, visibility);
        this.graph = graph;
        this.elementMutationBuilder = elementMutationBuilder;
        this.timestamp = timestamp;
    }

    @Override
    public Iterable<KeyValuePair> getEdgeTableKeyValuePairs() {
        Text rowKey = new Text(getId());
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(getVisibility());
        Mutation mutation = new Mutation(rowKey);
        mutation.put(AccumuloEdge.CF_SIGNAL, new Text(getLabel()), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        mutation.put(AccumuloEdge.CF_OUT_VERTEX, new Text(getVertexId(Direction.OUT)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        mutation.put(AccumuloEdge.CF_IN_VERTEX, new Text(getVertexId(Direction.IN)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        this.elementMutationBuilder.addElementMutationsToAccumuloMutation(this, getId(), mutation);

        return convertMutationToKeyValuePairs(rowKey, mutation);
    }

    private List<KeyValuePair> convertMutationToKeyValuePairs(Text rowKey, Mutation mutation) {
        return mutation.getUpdates().stream()
            .map(columnUpdate ->
                new KeyValuePair(
                    new Key(
                        rowKey,
                        new Text(columnUpdate.getColumnFamily()),
                        new Text(columnUpdate.getColumnQualifier()),
                        new Text(columnUpdate.getColumnVisibility()),
                        columnUpdate.getTimestamp()
                    ),
                    new Value(columnUpdate.getValue())
                )
            ).collect(Collectors.toList());
    }

    protected abstract AccumuloEdge createEdge(User user);

    @Override
    public Iterable<KeyValuePair> getVertexTableKeyValuePairs() {
        Text edgeIdText = new Text(getId());
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(getVisibility());
        Text edgeInfoVisibility = new Text(edgeColumnVisibility.getExpression());
        String label = graph.getNameSubstitutionStrategy().deflate(getLabel());

        org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo = new org.vertexium.accumulo.iterator.model.EdgeInfo(label, getVertexId(Direction.IN), edgeInfoVisibility);
        Text outRowKey = new Text(getVertexId(Direction.OUT));
        Mutation outMutation = new Mutation(outRowKey);
        outMutation.put(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, timestamp, edgeInfo.toValue());

        edgeInfo = new org.vertexium.accumulo.iterator.model.EdgeInfo(label, getVertexId(Direction.OUT), edgeInfoVisibility);
        Text inRowKey = new Text(getVertexId(Direction.IN));
        Mutation inMutation = new Mutation(inRowKey);
        inMutation.put(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility, timestamp, edgeInfo.toValue());

        ArrayList<KeyValuePair> results = new ArrayList<>();
        results.addAll(convertMutationToKeyValuePairs(outRowKey, outMutation));
        results.addAll(convertMutationToKeyValuePairs(inRowKey, inMutation));
        return results;
    }
}
