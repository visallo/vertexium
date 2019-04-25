package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public class ListLiteralExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final String[] argumentResultNames;

    public ListLiteralExecutionStep(String resultName, ExecutionStepWithResultName[] elementSteps) {
        super(elementSteps);
        this.resultName = resultName;
        this.argumentResultNames = ExecutionStepWithResultName.getResultNames(elementSteps);
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object[] arr = row.get(argumentResultNames);
            row.popScope(argumentResultNames.length);
            row.pushScope(getResultName(), arr);
        });
    }
}
