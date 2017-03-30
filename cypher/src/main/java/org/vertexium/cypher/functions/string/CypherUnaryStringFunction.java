package org.vertexium.cypher.functions.string;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public abstract class CypherUnaryStringFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);
        if (arg0 == null) {
            return null;
        }
        if (arg0 instanceof String) {
            return invokeOnString((String) arg0);
        }
        throw new VertexiumCypherTypeErrorException(arg0, String.class, null);
    }

    protected abstract Object invokeOnString(String str);
}
