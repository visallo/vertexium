package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public abstract class CypherUnaryMathFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof Double || arg0 instanceof Float) {
            return invokeDouble(ctx, ((Number) arg0).doubleValue(), scope);
        }

        if (arg0 instanceof Integer || arg0 instanceof Long) {
            return invokeLong(ctx, ((Number) arg0).longValue(), scope);
        }

        throw new VertexiumCypherTypeErrorException(arg0, Double.class, Float.class, Integer.class, Long.class);
    }

    protected Object invokeLong(VertexiumCypherQueryContext ctx, long value, ExpressionScope scope) {
        return invokeDouble(ctx, (double) value, scope);
    }

    protected abstract Object invokeDouble(VertexiumCypherQueryContext ctx, double value, ExpressionScope scope);
}
