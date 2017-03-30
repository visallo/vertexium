package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.utils.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SumFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);

        if (scope instanceof VertexiumCypherScope) {
            List<Object> list = new ArrayList<>();
            List<VertexiumCypherScope.Item> items = ((VertexiumCypherScope) scope).stream().collect(Collectors.toList());
            for (VertexiumCypherScope.Item item : items) {
                Object itemValue = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], item);
                list.add(itemValue);
            }
            return ObjectUtils.sumNumbers(list);
        }

        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof List) {
            List<?> list = (List<?>) arg0;
            return ObjectUtils.sumNumbers(list);
        }

        throw new VertexiumCypherTypeErrorException(arg0, List.class);
    }
}
