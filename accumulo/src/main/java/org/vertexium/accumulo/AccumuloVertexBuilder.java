package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;

import java.util.ArrayList;

import static org.vertexium.accumulo.AccumuloGraph.visibilityToAccumuloVisibility;
import static org.vertexium.accumulo.ElementMutationBuilder.EMPTY_VALUE;

public abstract class AccumuloVertexBuilder extends VertexBuilder implements VertexBuilderWithKeyValuePairs {
    private final ElementMutationBuilder elementMutationBuilder;
    private final Long timestamp;

    public AccumuloVertexBuilder(String vertexId, Visibility visibility, Long timestamp, ElementMutationBuilder elementMutationBuilder) {
        super(vertexId, visibility);
        this.elementMutationBuilder = elementMutationBuilder;
        this.timestamp = timestamp;
    }

    @Override
    public Iterable<KeyValuePair> getKeyValuePairs() {
        Text rowKey = new Text(getId());
        Mutation mutation = new Mutation(rowKey);
        this.elementMutationBuilder.addElementMutationsToAccumuloMutation(this, getId(), mutation);

        ArrayList<KeyValuePair> results = new ArrayList<>();
        results.add(new KeyValuePair(new Key(rowKey, AccumuloVertex.CF_SIGNAL, ElementMutationBuilder.EMPTY_TEXT, visibilityToAccumuloVisibility(getVisibility()), timestamp), EMPTY_VALUE));
        mutation.getUpdates()
            .forEach(columnUpdate ->
                results.add(new KeyValuePair(
                    new Key(
                        rowKey,
                        new Text(columnUpdate.getColumnFamily()),
                        new Text(columnUpdate.getColumnQualifier()),
                        new Text(columnUpdate.getColumnVisibility()),
                        columnUpdate.getTimestamp()
                    ),
                    new Value(columnUpdate.getValue())
                ))
            );
            return results;
    }

    @Override
    public abstract Vertex save(Authorizations authorizations);

    protected abstract AccumuloVertex createVertex(User user);

    public ElementMutationBuilder getElementMutationBuilder() {
        return elementMutationBuilder;
    }
}
