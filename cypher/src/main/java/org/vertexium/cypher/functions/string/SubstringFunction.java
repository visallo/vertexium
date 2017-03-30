package org.vertexium.cypher.functions.string;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class SubstringFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 2, 3);
        Object originalObj = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);
        Object startObj = ctx.getExpressionExecutor().executeExpression(ctx, arguments[1], scope);
        Object lengthObj = arguments.length > 2 ? ctx.getExpressionExecutor().executeExpression(ctx, arguments[2], scope) : null;

        int start;
        if (startObj instanceof Number) {
            start = ((Number) startObj).intValue();
        } else {
            throw new VertexiumCypherTypeErrorException(startObj, Number.class);
        }

        Integer length;
        if (lengthObj == null) {
            length = null;
        } else if (lengthObj instanceof Number) {
            length = ((Number) lengthObj).intValue();
        } else {
            throw new VertexiumCypherTypeErrorException(lengthObj, Number.class, null);
        }

        if (originalObj instanceof String) {
            String original = (String) originalObj;
            String result = original.substring(start);
            if (length != null) {
                result = result.substring(length);
            }
            return result;
        }

        throw new VertexiumCypherTypeErrorException(originalObj, String.class);
    }
}
