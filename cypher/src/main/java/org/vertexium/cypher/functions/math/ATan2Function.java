package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class ATan2Function extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 2);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);
        Object arg1 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[1], scope);
        if (!(arg0 instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(arg0, Number.class);
        }
        if (!(arg1 instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(arg1, Number.class);
        }
        return Math.atan2(((Number) arg0).doubleValue(), ((Number) arg1).doubleValue());
    }
}
