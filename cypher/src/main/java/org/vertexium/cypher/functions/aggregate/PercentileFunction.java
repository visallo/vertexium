package org.vertexium.cypher.functions.aggregate;

import com.google.common.util.concurrent.AtomicDouble;
import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherArgumentErrorException;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executionPlan.AggregationFunctionInvocationExecutionStep;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PercentileFunction implements AggregationFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        return new AggregationFunctionInvocationExecutionStep(getClass().getSimpleName(), resultName, distinct, argumentsExecutionStep) {
            @Override
            protected CypherResultRow executeAggregation(VertexiumCypherQueryContext ctx, CypherResultRow group, Stream<RowWithArguments> rows) {
                AtomicLong count = new AtomicLong();
                AtomicDouble percentile = new AtomicDouble();
                List<Double> values = rows
                    .map(row -> {
                        count.incrementAndGet();

                        Object arg1 = row.arguments[1];
                        VertexiumCypherTypeErrorException.assertType(arg1, Number.class);
                        double arg1Double = ((Number) arg1).doubleValue();
                        if (arg1Double < 0 || arg1Double > 1) {
                            throw new VertexiumCypherArgumentErrorException("NumberOutOfRange: percentile must be between 0.0 and 1.0");
                        }
                        percentile.set(arg1Double);

                        Object v = row.arguments[0];
                        if (v instanceof Number) {
                            return ((Number) v).doubleValue();
                        }
                        throw new VertexiumCypherTypeErrorException(v, Number.class);
                    })
                    .collect(Collectors.toList());

                Object result;
                if (count.get() == 0) {
                    result = null;
                } else {
                    result = invoke(ctx, values, percentile.get());
                }
                return group.clone()
                    .pushScope(getResultName(), result);
            }

            @Override
            protected int getExpectedArgumentCount() {
                return 2;
            }
        };
    }

    protected abstract Object invoke(VertexiumCypherQueryContext ctx, List<Double> values, double percentile);
}
