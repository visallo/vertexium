package org.vertexium.cypher.executor;

import org.vertexium.Element;
import org.vertexium.Property;
import org.vertexium.Vertex;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SetClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SetClauseExecutor.class);

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherSetClause clause, VertexiumCypherScope scope) {
        LOGGER.debug("execute: %s", clause);
        scope.run(); // TODO change the execute to peek and set the values instead of consuming the stream
        scope.stream()
            .forEach(item -> execute(ctx, clause, item));
        return scope;
    }

    public void execute(VertexiumCypherQueryContext ctx, CypherSetClause clause, VertexiumCypherScope.Item item) {
        clause.getSetItems().forEach(setItem -> {
            if (setItem instanceof CypherSetNodeLabels) {
                executeSetNodeLabels(ctx, (CypherSetNodeLabels) setItem, item);
            } else if (setItem instanceof CypherSetProperty) {
                executeSetProperty(ctx, (CypherSetProperty) setItem, item);
            } else if (setItem instanceof CypherSetVariable) {
                executeSetVariable(ctx, (CypherSetVariable) setItem, item);
            } else {
                throw new VertexiumCypherTypeErrorException(
                    setItem,
                    CypherSetNodeLabels.class,
                    CypherSetProperty.class,
                    CypherSetVariable.class
                );
            }
        });
    }

    private void executeSetVariable(
        VertexiumCypherQueryContext ctx,
        CypherSetVariable setItem,
        VertexiumCypherScope.Item item
    ) {
        Object left = ctx.getExpressionExecutor().executeExpression(ctx, setItem.getLeft(), item);
        VertexiumCypherTypeErrorException.assertType(left, Element.class, null);
        if (left == null) {
            return;
        }

        Object values = ctx.getExpressionExecutor().executeExpression(ctx, setItem.getRight(), item);
        CypherSetItem.Op op = setItem.getOp();

        if (values instanceof Stream) {
            values = ((Stream<?>) values).collect(Collectors.toList());
        }
        if (values instanceof List && ((List) values).size() == 1) {
            values = ((List) values).get(0);
        }
        VertexiumCypherTypeErrorException.assertType(values, Map.class, Element.class);

        if (values instanceof Element) {
            values = ctx.getElementPropertiesAsMap((Element) values);
        }

        Element element = (Element) left;
        //noinspection unchecked
        Map<String, ?> valuesMap = (Map<String, ?>) values;

        ExistingElementMutation<Element> m = element.prepareMutation();

        if (op == CypherSetItem.Op.EQUAL) {
            for (Property property : element.getProperties()) {
                if (ctx.isLabelProperty(property)) {
                    continue;
                }
                if (!valuesMap.containsKey(property.getName())) {
                    ctx.removeProperty(m, property.getName());
                }
            }
        }

        for (Map.Entry<String, ?> valuesMapEntry : valuesMap.entrySet()) {
            String propertyName = valuesMapEntry.getKey();
            Object value = valuesMapEntry.getValue();
            if (value instanceof CypherLiteral) {
                value = ((CypherLiteral) value).getValue();
            }
            if (value == null) {
                ctx.removeProperty(m, propertyName);
            } else {
                ctx.setProperty(m, propertyName, value);
            }
        }

        ctx.saveElement(m);
    }

    private void executeSetProperty(
        VertexiumCypherQueryContext ctx,
        CypherSetProperty setItem,
        VertexiumCypherScope.Item item
    ) {
        Object left = ctx.getExpressionExecutor().executeExpression(ctx, setItem.getLeft().getAtom(), item);
        VertexiumCypherTypeErrorException.assertType(left, Element.class, null);
        if (left == null) {
            return;
        }

        String propertyName = setItem.getLeft().getProperty();
        Object value = ctx.getExpressionExecutor().executeExpression(ctx, setItem.getRight(), item);

        Element element = (Element) left;
        ExistingElementMutation<Element> m = element.prepareMutation();
        switch (setItem.getOp()) {
            case EQUAL:
                if (value == null) {
                    ctx.removeProperty(m, propertyName);
                } else {
                    ctx.setProperty(m, propertyName, value);
                }
                break;
            default:
                throw new VertexiumCypherNotImplemented("" + setItem);
        }
        ctx.saveElement(m);
    }

    private void executeSetNodeLabels(
        VertexiumCypherQueryContext ctx,
        CypherSetNodeLabels setItem,
        VertexiumCypherScope.Item item
    ) {
        Object left = ctx.getExpressionExecutor().executeExpression(ctx, setItem.getLeft(), item);
        VertexiumCypherTypeErrorException.assertType(left, Vertex.class, null);
        if (left == null) {
            return;
        }

        Vertex vertex = (Vertex) left;
        ExistingElementMutation<Vertex> m = vertex.prepareMutation();
        for (CypherLabelName labelName : setItem.getRight()) {
            ctx.setLabelProperty(m, labelName.getValue());
        }
        ctx.saveVertex(m);
    }
}
