package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

import java.util.List;
import java.util.Set;

public class HeadFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 instanceof List) {
            List list = (List) arg0;
            if (list.size() == 0) {
                return null;
            }
            return list.get(0);
        }

        if (arg0 instanceof Set) {
            Set set = (Set) arg0;
            if (set.size() == 0) {
                return null;
            }
            return set.iterator().next();
        }

        throw new VertexiumCypherTypeErrorException(arg0, List.class, Set.class);
    }
}
