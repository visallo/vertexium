package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public class WithClauseExecutionStep extends ExecutionStepWithChildren {
    public WithClauseExecutionStep(ReturnExecutionStep returnBody, WhereExecutionStep whereExpression) {
        super(toChildren(returnBody, whereExpression));
    }

    private static ExecutionStep[] toChildren(ReturnExecutionStep returnBody, WhereExecutionStep whereExpression) {
        int length = 1 + (whereExpression == null ? 0 : 1);
        ExecutionStep[] result = new ExecutionStep[length];
        result[0] = returnBody;
        if (whereExpression != null) {
            result[result.length - 1] = whereExpression;
        }
        return result;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        if (source == null) {
            source = new SingleRowVertexiumCypherResult();
        }
        return super.execute(ctx, source);
    }
}
