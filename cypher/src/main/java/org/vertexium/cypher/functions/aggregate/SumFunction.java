package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.executionPlan.AggregationFunctionInvocationExecutionStep;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;
import org.vertexium.cypher.utils.ObjectUtils;

import java.util.stream.Stream;

public class SumFunction implements AggregationFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        return new AggregationFunctionInvocationExecutionStep(getClass().getSimpleName(), resultName, distinct, argumentsExecutionStep) {
            @Override
            protected CypherResultRow executeAggregation(VertexiumCypherQueryContext ctx, CypherResultRow group, Stream<RowWithArguments> rows) {
                if (rows == null) {
                    return group.clone()
                        .pushScope(getResultName(), null);
                }

                Number sumValue = rows
                    .filter(r -> r.arguments[0] != null)
                    .reduce(
                        null,
                        (number, row) -> {
                            if (number == null) {
                                number = 0L;
                            }
                            Object rowValue = row.arguments[0];
                            if (!(rowValue instanceof Number)) {
                                throw new VertexiumCypherException("Expected number. Found " + rowValue.getClass().getName());
                            }
                            return ObjectUtils.addNumbers(number, (Number) rowValue);
                        },
                        ObjectUtils::addNumbers
                    );
                return group.clone()
                    .pushScope(resultName, sumValue);
            }
        };
    }
}
