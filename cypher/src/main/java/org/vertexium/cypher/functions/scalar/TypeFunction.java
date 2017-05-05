package org.vertexium.cypher.functions.scalar;

import org.vertexium.Edge;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class TypeFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof Edge) {
            Edge arg0Edge = (Edge) arg0;
            return arg0Edge.getLabel();
        }

        throw new VertexiumCypherTypeErrorException(arg0, Edge.class, null);
    }
}
