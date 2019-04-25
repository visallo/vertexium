package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.executionPlan.AggregationFunctionInvocationExecutionStep;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;

import java.util.stream.Stream;

public class CollectFunction implements AggregationFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        return new AggregationFunctionInvocationExecutionStep(getClass().getSimpleName(), resultName, distinct, argumentsExecutionStep) {
            @Override
            protected CypherResultRow executeAggregation(VertexiumCypherQueryContext ctx, CypherResultRow group, Stream<RowWithArguments> rows) {
                Object[] value = rows
                    .map(r -> r.arguments[0])
                    .filter(r -> r != null)
                    .toArray(Object[]::new);
                return group.clone()
                    .pushScope(getResultName(), value);
            }
        };
    }
}
