package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

import java.util.stream.Stream;

public class NoReturnValueExecutionStep extends DefaultExecutionStep {
    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source.count();
        return new VertexiumCypherResult(Stream.empty(), source.getColumnNames());
    }
}
