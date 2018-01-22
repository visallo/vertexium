package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class HeadFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof Stream) {
            Stream<?> stream = (Stream<?>) arg0;
            return stream.findFirst().orElse(null);
        }

        if (arg0 instanceof List) {
            List list = (List) arg0;
            if (list.size() == 0) {
                return null;
            }
            return list.get(0);
        }

        if (arg0 instanceof Collection) {
            Collection collection = (Collection) arg0;
            if (collection.size() == 0) {
                return null;
            }
            return collection.iterator().next();
        }

        throw new VertexiumCypherTypeErrorException(arg0, Collection.class, Stream.class);
    }
}
