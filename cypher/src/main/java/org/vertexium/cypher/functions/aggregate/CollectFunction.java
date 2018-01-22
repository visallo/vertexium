package org.vertexium.cypher.functions.aggregate;

import com.google.common.collect.Lists;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.ast.model.CypherLiteral;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arguments[0] instanceof CypherLiteral) {
            return Lists.newArrayList(arg0);
        }

        if (arg0 instanceof Collection) {
            arg0 = ((Collection) arg0).stream();
        }

        if (arg0 instanceof Stream) {
            return ((Stream<?>) arg0)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        throw new VertexiumCypherTypeErrorException(arg0, Collection.class, Stream.class);
    }
}
