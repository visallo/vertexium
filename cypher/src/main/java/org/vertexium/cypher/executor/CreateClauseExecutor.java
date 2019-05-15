package org.vertexium.cypher.executor;

import org.vertexium.*;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(CreateClauseExecutor.class);
    private final ExpressionExecutor expressionExecutor;

    public CreateClauseExecutor(ExpressionExecutor expressionExecutor) {
        this.expressionExecutor = expressionExecutor;
    }

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherCreateClause clause, VertexiumCypherScope scope) {
        LOGGER.debug("execute: %s", clause);
        scope.run(); // materialize existing scope to prevent new items being returned as if they matched the previous step
        Stream<VertexiumCypherScope.Item> results = scope.stream()
            .map(item -> executeCreate(ctx, clause, item));
        return VertexiumCypherScope.newItemsScope(results, scope);
    }

    private VertexiumCypherScope.Item executeCreate(
        VertexiumCypherQueryContext ctx,
        CypherCreateClause createClause,
        VertexiumCypherScope.Item item
    ) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (CypherPatternPart cypherPatternPart : createClause.getPatternParts()) {
            LinkedHashMap<String, Element> patternPartResult = executePatternPart(
                ctx,
                cypherPatternPart,
                VertexiumCypherScope.newMapItem(map, item)
            );
            map.putAll(patternPartResult);
        }
        return VertexiumCypherScope.newMapItem(map, item);
    }

    public LinkedHashMap<String, Element> executePatternPart(
        VertexiumCypherQueryContext ctx,
        CypherPatternPart cypherPatternPart,
        VertexiumCypherScope.Item item
    ) {
        LinkedHashMap<String, Element> elements = new LinkedHashMap<>();
        Vertex lastVertex = null;
        CypherRelationshipPattern lastRelationshipPattern = null;
        for (CypherElementPattern cypherElementPattern : cypherPatternPart.getElementPatterns()) {
            if (cypherElementPattern instanceof CypherNodePattern) {
                Vertex vertex = null;
                String elementName = cypherElementPattern.getName();
                if (elementName != null) {
                    vertex = lookupExistingElement(elementName, elements, Vertex.class, item);
                }

                if (vertex == null) {
                    vertex = executeCreateVertex(ctx, (CypherNodePattern) cypherElementPattern, item);
                    if (elementName != null) {
                        elements.put(elementName, vertex);
                    }
                } else {
                    executeUpdateVertex(ctx, (CypherNodePattern) cypherElementPattern, vertex, item);
                }

                if (lastVertex != null && lastRelationshipPattern != null) {
                    Edge edge = null;
                    String edgeName = lastRelationshipPattern.getName();
                    if (edgeName != null) {
                        edge = lookupExistingElement(edgeName, elements, Edge.class, item);
                    }
                    if (edge == null) {
                        edge = executeCreateEdge(ctx, lastRelationshipPattern, lastVertex, vertex, item);
                        if (edgeName != null) {
                            elements.put(edgeName, edge);
                        }
                    }
                }
                lastVertex = vertex;
            } else if (cypherElementPattern instanceof CypherRelationshipPattern) {
                lastRelationshipPattern = (CypherRelationshipPattern) cypherElementPattern;
            } else {
                throw new VertexiumException("Unexpected element pattern: " + cypherElementPattern.getClass().getName());
            }
        }
        return elements;
    }

    private <T extends Element> T lookupExistingElement(
        String name,
        Map<String, Element> elements,
        Class<T> resultType,
        ExpressionScope scope
    ) {
        Element element;
        if (scope == null) {
            return null;
        }
        element = elements.get(name);
        if (element == null) {
            Object obj = scope.getByName(name);
            if (obj instanceof Stream) {
                Stream<?> stream = (Stream<?>) obj;
                obj = stream.collect(Collectors.toList());
            }
            if (obj instanceof List) {
                List list = (List) obj;
                if (list.size() == 0) {
                    return null;
                }
                if (list.size() == 1) {
                    obj = list.get(0);
                }
            }
            if (obj != null && !(obj instanceof Element)) {
                throw new VertexiumException("Expected Element with name \"" + name + "\", found \"" + obj.getClass().getName() + "\"");
            }
            element = (Element) obj;
        }
        if (element == null) {
            return null;
        }
        return resultType.cast(element);
    }

    private Edge executeCreateEdge(
        VertexiumCypherQueryContext ctx,
        CypherRelationshipPattern relationshipPattern,
        Vertex leftVertex,
        Vertex rightVertex,
        VertexiumCypherScope.Item item
    ) {
        CypherDirection direction = relationshipPattern.getDirection();
        Vertex outVertex;
        Vertex inVertex;
        if (direction == CypherDirection.OUT) {
            outVertex = leftVertex;
            inVertex = rightVertex;
        } else if (direction == CypherDirection.IN) {
            outVertex = rightVertex;
            inVertex = leftVertex;
        } else {
            throw new VertexiumException("unexpected direction: " + direction);
        }
        String edgeId = ctx.calculateEdgeId(relationshipPattern, item);
        String label = ctx.calculateEdgeLabel(relationshipPattern, outVertex, inVertex, item);
        label = ctx.normalizeLabelName(label);
        Visibility visibility = ctx.calculateEdgeVisibility(relationshipPattern, outVertex, inVertex, item);
        EdgeBuilder m = ctx.getGraph().prepareEdge(edgeId, outVertex, inVertex, label, visibility);
        setPropertiesOnElement(ctx, m, relationshipPattern, item);
        return ctx.getGraph().getEdge(ctx.saveEdge(m), ctx.getFetchHints(), ctx.getUser());
    }

    private Vertex executeCreateVertex(
        VertexiumCypherQueryContext ctx,
        CypherNodePattern nodePattern,
        VertexiumCypherScope.Item item
    ) {
        String vertexId = ctx.calculateVertexId(nodePattern, item);
        Visibility vertexVisibility = ctx.calculateVertexVisibility(nodePattern, item);
        VertexBuilder m = ctx.getGraph().prepareVertex(vertexId, vertexVisibility);
        updateVertex(ctx, m, nodePattern, item);
        return ctx.getGraph().getVertex(ctx.saveVertex(m), ctx.getFetchHints(), ctx.getUser());
    }

    private void executeUpdateVertex(
        VertexiumCypherQueryContext ctx,
        CypherNodePattern nodePattern,
        Vertex vertex,
        VertexiumCypherScope.Item item
    ) {
        ExistingElementMutation<Vertex> m = vertex.prepareMutation();
        updateVertex(ctx, m, nodePattern, item);
        ctx.saveVertex(m);
    }

    private void updateVertex(
        VertexiumCypherQueryContext ctx,
        ElementMutation<Vertex> m,
        CypherNodePattern nodePattern,
        VertexiumCypherScope.Item item
    ) {
        for (CypherLabelName label : nodePattern.getLabelNames()) {
            String labelName = ctx.normalizeLabelName(label.getValue());
            ctx.setLabelProperty(m, labelName);
        }
        setPropertiesOnElement(ctx, m, nodePattern, item);
    }

    private <T extends Element> void setPropertiesOnElement(
        VertexiumCypherQueryContext ctx,
        ElementMutation<T> m,
        CypherElementPattern elementPattern,
        VertexiumCypherScope.Item item
    ) {
        for (Map.Entry<String, CypherAstBase> property : elementPattern.getPropertiesMap().entrySet()) {
            String propertyName = ctx.normalizePropertyName(property.getKey());
            CypherAstBase propertyValue = property.getValue();
            Object value = toObject(ctx, propertyValue, item);
            if (value != null) {
                ctx.setProperty(m, propertyName, value);
            }
        }
    }

    private Object toObject(VertexiumCypherQueryContext ctx, CypherAstBase expression, VertexiumCypherScope.Item item) {
        return expressionExecutor.executeExpression(ctx, expression, item);
    }
}
