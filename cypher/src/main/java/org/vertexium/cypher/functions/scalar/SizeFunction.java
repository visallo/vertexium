package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

import java.util.Collection;

public class SizeFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg instanceof Collection) {
            return ((Collection) arg).size();
        }

        throw new VertexiumCypherTypeErrorException(arg, Collection.class);
    }
}
