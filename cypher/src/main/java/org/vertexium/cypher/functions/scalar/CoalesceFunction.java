package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class CoalesceFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        for (CypherAstBase argument : arguments) {
            Object o = ctx.getExpressionExecutor().executeExpression(ctx, argument, scope);
            if (o != null) {
                return o;
            }
        }
        return null;
    }
}
