package org.vertexium.cypher.functions.aggregate;

import org.vertexium.Element;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;

import java.util.List;
import java.util.Objects;

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

        if (arg0 instanceof List) {
            return ((List<?>) arg0).stream()
                    .filter(Objects::nonNull)
                    .count();
        }

        return 1;
    }
}
