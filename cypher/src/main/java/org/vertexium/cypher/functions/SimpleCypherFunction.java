package org.vertexium.cypher.functions;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;
import org.vertexium.cypher.executionPlan.FunctionInvocationExecutionStep;

public abstract class SimpleCypherFunction implements CypherFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        if (distinct) {
            throw new VertexiumCypherNotImplemented("distinct");
        }
        return new FunctionInvocationExecutionStep(getClass().getSimpleName(), resultName, argumentsExecutionStep) {
            @Override
            protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
                return SimpleCypherFunction.this.executeFunction(ctx, arguments);
            }
        };
    }

    protected abstract Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments);
}
