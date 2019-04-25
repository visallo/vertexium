package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

public class IndexedParameterExecutionStep extends DefaultExecutionStep implements ExecutionStepWithResultName {
    private final String resultName;
    private final int index;

    public IndexedParameterExecutionStep(String resultName, int index) {
        this.resultName = resultName;
        this.index = index;
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        return source.peek(row -> {
            throw new VertexiumCypherNotImplemented("index");
        });
    }
}
