package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.utils.ObjectUtils;
import org.vertexium.util.StreamUtils;

import java.util.Collection;
import java.util.stream.Stream;

public class AverageFunction extends AggregationFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof Collection) {
            Collection<?> list = (Collection<?>) arg0;
            if (list.size() == 0) {
                return null;
            }
            return ObjectUtils.sumNumbers(list.stream()).doubleValue() / (double) list.size();
        }

        if (arg0 instanceof Stream) {
            Stream<?> list = (Stream<?>) arg0;
            return StreamUtils.ifEmpty(
                list,
                () -> null,
                ObjectUtils::averageNumbers
            );
        }

        throw new VertexiumCypherTypeErrorException(arg0, Collection.class, Stream.class);
    }
}
