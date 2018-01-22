package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.utils.ObjectUtils;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

public class MaxFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof Collection) {
            arg0 = ((Collection) arg0).stream();
        }

        if (arg0 instanceof Stream) {
            Stream<?> list = (Stream<?>) arg0;
            return list
                    .filter(Objects::nonNull)
                    .max(ObjectUtils::compare)
                    .orElse(null);
        }

        throw new VertexiumCypherTypeErrorException(arg0, Collection.class, Stream.class);
    }
}
