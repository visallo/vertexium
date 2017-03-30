package org.vertexium.cypher.functions.predicate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class ExistsFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        CypherAstBase lookup = arguments[0];
        Object value = ctx.getExpressionExecutor().executeExpression(ctx, lookup, scope);
        return value != null;
    }
}
