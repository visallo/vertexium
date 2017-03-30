package org.vertexium.cypher.functions.list;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherArgumentErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class RangeFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        if (arguments.length > 3) {
            throw new VertexiumException("range function takes 2 or 3 arguments");
        }
        int arg0 = argumentToInt(ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope));
        int arg1 = argumentToInt(ctx.getExpressionExecutor().executeExpression(ctx, arguments[1], scope));
        Stream<Integer> result = IntStream.rangeClosed(arg0, arg1).boxed();
        if (arguments.length == 3) {
            int step = argumentToInt(ctx.getExpressionExecutor().executeExpression(ctx, arguments[2], scope));
            if (step == 0) {
                throw new VertexiumCypherArgumentErrorException("NumberOutOfRange: step must be greater than 0");
            }
            result = result.filter(i -> i % step == 0);
        }
        return result;
    }

    private int argumentToInt(Object argument) {
        if (argument instanceof Long) {
            return (int) ((long) argument);
        }
        if (argument instanceof Integer) {
            return (int) argument;
        }
        throw new VertexiumException("Could not convert argument \"" + argument + "\" to int");
    }
}
