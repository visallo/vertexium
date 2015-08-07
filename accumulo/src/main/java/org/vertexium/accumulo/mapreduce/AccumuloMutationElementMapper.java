package org.vertexium.accumulo.mapreduce;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public abstract class AccumuloMutationElementMapper<KEYIN, VALUEIN> extends ElementMapper<KEYIN, VALUEIN, Text, Mutation> {
    @Override
    protected void saveDataMutation(Context context, Text dataTableName, Mutation m) throws IOException, InterruptedException {
        context.write(getKey(context, dataTableName, m), m);
    }

    @Override
    protected void saveEdgeMutation(Context context, Text edgesTableName, Mutation m) throws IOException, InterruptedException {
        context.write(getKey(context, edgesTableName, m), m);
    }

    @Override
    protected void saveVertexMutation(Context context, Text verticesTableName, Mutation m) throws IOException, InterruptedException {
        context.write(getKey(context, verticesTableName, m), m);
    }

    protected Text getKey(Context context, Text tableName, Mutation m) {
        return tableName;
    }
}
