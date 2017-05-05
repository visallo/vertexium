package org.vertexium.cypher.functions;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.executor.ExpressionScope;

public abstract class TypeConversionFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        Object value = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);
        return convert(value);
    }

    protected abstract Object convert(Object value);
}
