package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

public class CountFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof String && arg0.equals("*")) {
            if (scope instanceof VertexiumCypherScope) {
                return ((VertexiumCypherScope) scope).stream()
                    .filter(Objects::nonNull)
                    .count();
            } else if (scope instanceof VertexiumCypherScope.Item) {
                return 1;
            }
            throw new VertexiumCypherTypeErrorException(scope, VertexiumCypherScope.class, VertexiumCypherScope.Item.class);
        }

        if (arg0 instanceof Collection) {
            arg0 = ((Collection) arg0).stream();
        }

        if (arg0 instanceof Stream) {
            return ((Stream<?>) arg0)
                .filter(Objects::nonNull)
                .count();
        }

        return 1;
    }
}
