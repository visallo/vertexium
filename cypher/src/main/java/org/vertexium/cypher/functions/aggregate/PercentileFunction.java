package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherArgumentErrorException;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;

import java.util.List;
import java.util.stream.Collectors;

public abstract class PercentileFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 2);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);
        Object arg1 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[1], scope);
        VertexiumCypherTypeErrorException.assertType(arg1, Number.class);

        double percentile = ((Number) arg1).doubleValue();
        if (percentile < 0 || percentile > 1) {
            throw new VertexiumCypherArgumentErrorException("NumberOutOfRange: percentile must be between 0.0 and 1.0");
        }

        if (arg0 instanceof List) {
            List<Double> values = ((List<?>) arg0).stream()
                    .map(v -> {
                        if (v instanceof Number) {
                            return ((Number) v).doubleValue();
                        }
                        throw new VertexiumCypherTypeErrorException(v, Number.class);
                    })
                    .collect(Collectors.toList());
            return invoke(ctx, values, percentile, scope);
        }

        throw new VertexiumCypherTypeErrorException(arg0, List.class);
    }

    protected abstract Object invoke(VertexiumCypherQueryContext ctx, List<Double> values, double percentile, ExpressionScope scope);
}
