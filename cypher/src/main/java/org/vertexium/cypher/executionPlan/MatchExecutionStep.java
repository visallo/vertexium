package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public class MatchExecutionStep extends ExecutionStepWithChildren {
    public MatchExecutionStep(PatternPartExecutionStep[] childSteps, WhereExecutionStep whereStep) {
        super(toChildSteps(childSteps, whereStep));
    }

    private static ExecutionStep[] toChildSteps(PatternPartExecutionStep[] childSteps, WhereExecutionStep whereStep) {
        int length = childSteps.length + (whereStep == null ? 0 : 1);
        ExecutionStep[] results = new ExecutionStep[length];
        System.arraycopy(childSteps, 0, results, 0, childSteps.length);
        if (whereStep != null) {
            results[results.length - 1] = whereStep;
        }
        return results;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        return super.execute(ctx, source);
    }

    @Override
    public String toString() {
        return String.format("%s", super.toString());
    }
}
