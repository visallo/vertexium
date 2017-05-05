package org.vertexium.cypher.functions.list;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class NodesFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof VertexiumCypherScope.PathItem) {
            VertexiumCypherScope.PathItem pathResult = (VertexiumCypherScope.PathItem) arg0;
            return pathResult.getVertices();
        }

        throw new VertexiumCypherTypeErrorException(arg0, VertexiumCypherScope.PathItem.class, null);
    }
}
