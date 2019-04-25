package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public class GetVariableExecutionStep extends DefaultExecutionStep implements ExecutionStepWithResultName {
    private final String resultName;
    private final String name;

    public GetVariableExecutionStep(String resultName, String name) {
        this.resultName = resultName;
        this.name = name;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        return new VertexiumCypherResult(
            source.peek(row -> {
                row.pushScope(resultName, row.get(name));
            }),
            source.getColumnNames()
        );
    }

    @Override
    public String toString() {
        return String.format("%s {name=%s, resultName=%s}", super.toString(), name, resultName);
    }

    @Override
    public String getResultName() {
        return resultName;
    }
}
