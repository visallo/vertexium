package org.vertexium.cypher.executor;

import org.vertexium.Element;
import org.vertexium.Vertex;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

public class RemoveClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(RemoveClauseExecutor.class);

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherRemoveClause clause, VertexiumCypherScope scope) {
        LOGGER.debug("execute: %s", clause);

        scope.stream()
                .forEach(item -> execute(ctx, clause, item));
        return scope;
    }

    private void execute(VertexiumCypherQueryContext ctx, CypherRemoveClause clause, VertexiumCypherScope.Item item) {
        for (CypherRemoveItem removeItem : clause.getRemoveItems()) {
            executeRemoveItem(ctx, removeItem, item);
        }
    }

    private void executeRemoveItem(VertexiumCypherQueryContext ctx, CypherRemoveItem removeItem, VertexiumCypherScope.Item item) {
        if (removeItem instanceof CypherRemoveLabelItem) {
            executeRemoveLabelItem(ctx, (CypherRemoveLabelItem) removeItem, item);
            return;
        }

        if (removeItem instanceof CypherRemovePropertyExpressionItem) {
            executeRemoveProperty(ctx, (CypherRemovePropertyExpressionItem) removeItem, item);
            return;
        }

        throw new VertexiumCypherTypeErrorException(removeItem, CypherRemoveLabelItem.class);
    }

    private void executeRemoveProperty(
            VertexiumCypherQueryContext ctx,
            CypherRemovePropertyExpressionItem removeItem,
            VertexiumCypherScope.Item item
    ) {
        CypherAstBase propertyExpression = removeItem.getPropertyExpression();

        if (propertyExpression instanceof CypherLookup) {
            CypherLookup cypherLookup = (CypherLookup) propertyExpression;
            Object elementObj = ctx.getExpressionExecutor().executeExpression(ctx, cypherLookup.getAtom(), item);

            if (elementObj == null) {
                return;
            }

            if (elementObj instanceof Element) {
                Element element = (Element) elementObj;
                ExistingElementMutation<Element> m = element.prepareMutation();
                ctx.removeProperty(m, cypherLookup.getProperty());
                ctx.saveElement(m);
                return;
            }

            throw new VertexiumCypherTypeErrorException(elementObj, Element.class, null);
        }

        throw new VertexiumCypherTypeErrorException(propertyExpression, CypherLookup.class);
    }

    private void executeRemoveLabelItem(
            VertexiumCypherQueryContext ctx,
            CypherRemoveLabelItem removeItem,
            VertexiumCypherScope.Item item
    ) {
        Object vertexObj = ctx.getExpressionExecutor().executeExpression(ctx, removeItem.getVariable(), item);

        if (vertexObj == null) {
            return;
        }

        if (vertexObj instanceof Vertex) {
            ExistingElementMutation<Vertex> m = ((Vertex) vertexObj).prepareMutation();
            for (CypherLabelName labelName : removeItem.getLabelNames()) {
                ctx.removeLabel(m, labelName.getValue());
            }
            ctx.saveVertex(m);
            return;
        }

        throw new VertexiumCypherTypeErrorException(vertexObj, Vertex.class, null);
    }
}
