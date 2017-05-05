package org.vertexium.cypher.functions.list;

import com.google.common.collect.Lists;
import org.vertexium.Edge;
import org.vertexium.Vertex;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.cypher.functions.CypherFunction;

public class LabelsFunction extends CypherFunction {
    @Override
    public Object invoke(VertexiumCypherQueryContext ctx, CypherAstBase[] arguments, ExpressionScope scope) {
        CypherAstBase lookup = arguments[0];

        Object item = ctx.getExpressionExecutor().executeExpression(ctx, lookup, scope);
        if (item == null) {
            throw new VertexiumException("Could not find Vertex using " + lookup);
        }

        if (item instanceof Vertex) {
            Vertex vertex = (Vertex) item;
            return ctx.getVertexLabels(vertex);
        }

        if (item instanceof Edge) {
            Edge edge = (Edge) item;
            return Lists.newArrayList(edge.getLabel());
        }

        throw new VertexiumCypherTypeErrorException(item, Vertex.class, Edge.class);
    }
}
