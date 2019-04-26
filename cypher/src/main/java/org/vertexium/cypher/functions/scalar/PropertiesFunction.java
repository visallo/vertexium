package org.vertexium.cypher.functions.scalar;

import org.vertexium.Element;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.ast.model.CypherMapLiteral;
import org.vertexium.cypher.ast.model.CypherVariable;
import org.vertexium.cypher.exceptions.VertexiumCypherSyntaxErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

import java.util.Map;

public class PropertiesFunction extends CypherFunction {
    @Override
    public void compile(CypherCompilerContext compilerContext, CypherAstBase[] arguments) {
        CypherAstBase arg0 = arguments[0];
        if (arg0 == null
            || arg0 instanceof CypherVariable
            || arg0 instanceof CypherMapLiteral) {
            return;
        }

        throw new VertexiumCypherSyntaxErrorException("InvalidArgumentType: properties(): expected variable or map, found " + arg0.getClass().getName());
    }

    @Override
    public Map<String, Object> invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        assertArgumentCount(arguments, 1);
        Object arg0 = ctx.getExpressionExecutor().executeExpression(ctx, arguments[0], scope);

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof Element) {
            return ctx.getElementPropertiesAsMap((Element) arg0);
        }

        if (arg0 instanceof Map) {
            return getPropertiesFromMap(ctx, (Map) arg0);
        }

        throw new VertexiumException("not implemented");
    }

    private Map<String, Object> getPropertiesFromMap(VertexiumCypherQueryContext ctx, Map map) {
        return map;
    }
}
