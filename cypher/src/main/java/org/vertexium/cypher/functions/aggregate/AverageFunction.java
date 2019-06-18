package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.executionPlan.AggregationFunctionInvocationExecutionStep;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;
import org.vertexium.cypher.utils.ObjectUtils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class AverageFunction implements AggregationFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        return new AggregationFunctionInvocationExecutionStep(getClass().getSimpleName(), resultName, distinct, argumentsExecutionStep) {
            @Override
            protected CypherResultRow executeAggregation(VertexiumCypherQueryContext ctx, CypherResultRow group, Stream<RowWithArguments> rows) {
                if (rows == null) {
                    return group.clone()
                        .pushScope(getResultName(), null);
                }

                AtomicLong count = new AtomicLong();
                Number sum = rows
                    .filter(row -> row.arguments[0] != null)
                    .peek(row -> count.incrementAndGet())
                    .map(row -> (Number) row.arguments[0])
                    .reduce(0, ObjectUtils::addNumbers);
                Object result;
                if (count.get() == 0) {
                    result = null;
                } else {
                    result = ObjectUtils.divideNumbers(sum, (double) count.get());
                }
                return group.clone()
                    .pushScope(getResultName(), result);
            }
        };
    }
}
