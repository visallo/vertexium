package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public abstract class FunctionInvocationExecutionStep extends FunctionInvocationExecutionStepBase {
    public FunctionInvocationExecutionStep(
        String functionName,
        String resultName,
        ExecutionStepWithResultName[] argumentsExecutionStep
    ) {
        super(functionName, resultName, argumentsExecutionStep);
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> executeOnRow(ctx, row));
    }

    protected void executeOnRow(VertexiumCypherQueryContext ctx, CypherResultRow row) {
        Object result = executeFunction(ctx, getArguments(row));
        row.pushScope(getResultName(), result);
    }

    protected abstract Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments);
}
