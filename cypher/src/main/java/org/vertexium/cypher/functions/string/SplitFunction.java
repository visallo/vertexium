package org.vertexium.cypher.functions.string;

import com.google.common.collect.Lists;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class SplitFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 2);
        Object stringArgObj = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);
        Object delimiterArgObj = ctx.getExpressionExecutor().executeExpression(ctx, arguments[1], scope);

        if (!(stringArgObj instanceof String)) {
            throw new VertexiumException("Expected a string as the first argument, found " + stringArgObj.getClass().getName());
        }
        String stringArg = (String) stringArgObj;

        if (!(delimiterArgObj instanceof String)) {
            throw new VertexiumException("Expected a string as the second argument, found " + stringArgObj.getClass().getName());
        }
        String delimiterArg = (String) delimiterArgObj;

        return Lists.newArrayList(stringArg.split(delimiterArg));
    }
}
