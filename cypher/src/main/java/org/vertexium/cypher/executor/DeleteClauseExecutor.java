package org.vertexium.cypher.executor;

import org.vertexium.*;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherDeleteClause;
import org.vertexium.cypher.exceptions.VertexiumCypherConstraintVerificationFailedException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeleteClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(DeleteClauseExecutor.class);
    private final ExpressionExecutor expressionExecutor;

    public DeleteClauseExecutor(ExpressionExecutor expressionExecutor) {
        this.expressionExecutor = expressionExecutor;
    }

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherDeleteClause clause, VertexiumCypherScope scope) {
        LOGGER.debug("execute: %s", clause);
        scope.run(); // TODO change the execute to peek and delete instead of consuming the stream
        Stream<DeleteElementItem> elementsToDelete = scope.stream()
            .flatMap(item -> execute(ctx, clause, item).stream());
        deleteElements(ctx, elementsToDelete);
        return scope;
    }

    private List<DeleteElementItem> execute(
        VertexiumCypherQueryContext ctx,
        CypherDeleteClause clause,
        VertexiumCypherScope.Item item
    ) {
        List<DeleteElementItem> elementsToDelete = new ArrayList<>();
        clause.getExpressions().forEach(expr -> {
            Object exprResult = expressionExecutor.executeExpression(ctx, expr, item);
            if (exprResult == null) {
                return;
            }
            if (exprResult instanceof Element) {
                Element element = (Element) exprResult;
                elementsToDelete.add(new DeleteElementItem(element, clause.isDetach()));
            } else if (exprResult instanceof VertexiumCypherScope.PathItem) {
                VertexiumCypherScope.PathItem path = (VertexiumCypherScope.PathItem) exprResult;
                if (clause.isDetach()) {
                    for (Vertex vertex : path.getVertices()) {
                        elementsToDelete.add(new DeleteElementItem(vertex, clause.isDetach()));
                    }
                } else {
                    for (Edge edge : path.getEdges()) {
                        elementsToDelete.add(new DeleteElementItem(edge, clause.isDetach()));
                    }
                    for (Vertex vertex : path.getVertices()) {
                        elementsToDelete.add(new DeleteElementItem(vertex, clause.isDetach()));
                    }
                }
            } else {
                throw new VertexiumException("not implemented \"" + exprResult.getClass().getName() + "\": " + exprResult);
            }
        });
        return elementsToDelete;
    }

    private void deleteElements(VertexiumCypherQueryContext ctx, Stream<DeleteElementItem> elementsToDelete) {
        List<DeleteElementItem> elementsToDeleteList = elementsToDelete.collect(Collectors.toList());
        for (DeleteElementItem deleteElementItem : elementsToDeleteList) {
            deleteElement(ctx, deleteElementItem.element, deleteElementItem.detach, elementsToDeleteList.stream());
        }
    }

    private void deleteElement(
        VertexiumCypherQueryContext ctx,
        Element element,
        boolean detach,
        Stream<DeleteElementItem> elementsToDelete
    ) {
        if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;
            deleteVertex(ctx, vertex, detach, elementsToDelete);
        } else if (element instanceof Edge) {
            Edge edge = (Edge) element;
            deleteEdge(ctx, edge);
        } else {
            throw new VertexiumException("unhandled element type to delete: " + element.getClass().getName());
        }
    }

    private void deleteVertex(
        VertexiumCypherQueryContext ctx,
        Vertex vertex,
        boolean detach,
        Stream<DeleteElementItem> elementsToDelete
    ) {
        if (!detach && isAttached(ctx, vertex, elementsToDelete)) {
            throw new VertexiumCypherConstraintVerificationFailedException(
                "DeleteConnectedNode: Cannot delete a vertex with edges unless 'DETACH' is specified in the 'DELETE' clause."
            );
        }
        ctx.deleteVertex(vertex);
    }

    private boolean isAttached(VertexiumCypherQueryContext ctx, Vertex vertex, Stream<DeleteElementItem> elementsToDelete) {
        for (String vertexId : vertex.getVertexIds(Direction.BOTH, ctx.getUser()).collect(Collectors.toList())) {
            if (elementsToDelete.noneMatch(e -> vertexId.equals(e.element.getId()))) {
                return true;
            }
        }
        return false;
    }

    private void deleteEdge(VertexiumCypherQueryContext ctx, Edge edge) {
        ctx.deleteEdge(edge);
    }

    private static class DeleteElementItem {
        public final Element element;
        public final boolean detach;

        public DeleteElementItem(Element element, boolean detach) {
            this.element = element;
            this.detach = detach;
        }
    }
}
