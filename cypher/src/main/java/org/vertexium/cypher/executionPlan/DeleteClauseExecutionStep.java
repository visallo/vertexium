package org.vertexium.cypher.executionPlan;

import org.vertexium.Edge;
import org.vertexium.Vertex;
import org.vertexium.cypher.PathResultBase;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherException;

public class DeleteClauseExecutionStep extends ExecutionStepWithChildren {
    private final boolean detach;
    private final String expressionResultName;

    public DeleteClauseExecutionStep(boolean detach, ExecutionStepWithResultName expression) {
        super(expression);
        this.detach = detach;
        this.expressionResultName = expression.getResultName();
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object value = row.get(expressionResultName);
            if (value == null) {
                return;
            }
            if (value instanceof Vertex) {
                Vertex vertex = (Vertex) value;
                if (!detach && vertex.getEdgesSummary(ctx.getUser()).getCountOfEdges() > 0) {
                    throw new VertexiumCypherException("Cannot delete vertex with edges unless you specify detach");
                }
                ctx.deleteVertex(vertex);
            } else if (value instanceof Edge) {
                Edge edge = (Edge) value;
                ctx.deleteEdge(edge);
            } else if (value instanceof PathResultBase) {
                ((PathResultBase) value).getElements().forEach(element -> {
                    if (element instanceof Vertex) {
                        ctx.deleteVertex((Vertex) element);
                    } else if (element instanceof Edge) {
                        ctx.deleteEdge((Edge) element);
                    } else {
                        throw new VertexiumCypherException("Unhandled element type: " + element.getClass().getName());
                    }
                });
            } else {
                throw new VertexiumCypherException("Expected element or " + PathResultBase.class.getSimpleName() + ", found " + value.getClass().getName());
            }
        });
    }
}
