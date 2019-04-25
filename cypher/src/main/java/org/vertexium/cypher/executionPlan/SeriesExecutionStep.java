package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public class SeriesExecutionStep extends ExecutionStepWithChildren {
    public SeriesExecutionStep(ExecutionStep... children) {
        super(children);
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        return super.execute(ctx, source);
    }
}
