package org.vertexium.cypher.functions.aggregate;

import com.google.common.collect.Lists;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.ast.model.CypherLiteral;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CollectFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arguments[0] instanceof CypherLiteral) {
            return Lists.newArrayList(arg0);
        }

        if (arg0 instanceof List) {
            return ((List<?>) arg0).stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        throw new VertexiumCypherTypeErrorException(arg0, List.class);
    }
}
