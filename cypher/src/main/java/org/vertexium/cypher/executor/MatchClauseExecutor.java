package org.vertexium.cypher.executor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executor.models.match.*;
import org.vertexium.cypher.executor.utils.MatchConstraintBuilder;
import org.vertexium.cypher.utils.ObjectUtils;
import org.vertexium.query.Contains;
import org.vertexium.search.EmptyQueryResults;
import org.vertexium.search.QueryResults;
import org.vertexium.util.StreamUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MatchClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(MatchClauseExecutor.class);

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, List<CypherMatchClause> matchClauses, VertexiumCypherScope scope) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute: %s", matchClauses.stream().map(CypherMatchClause::toString).collect(Collectors.joining("; ")));
        }
        MatchConstraints matchConstraints = new MatchConstraintBuilder().getMatchConstraints(matchClauses);
        Stream<VertexiumCypherScope.Item> results = scope.stream()
            .flatMap(item -> executeMatchConstraints(ctx, matchConstraints, item));
        return VertexiumCypherScope.newItemsScope(results, scope);
    }

    private Stream<VertexiumCypherScope.Item> executeMatchConstraints(
        VertexiumCypherQueryContext ctx,
        MatchConstraints matchConstraints,
        ExpressionScope scope
    ) {
        Stream<VertexiumCypherScope.Item> results = null;

        for (PatternPartMatchConstraint patternPartMatchConstraint : matchConstraints.getPatternPartMatchConstraints()) {
            Stream<VertexiumCypherScope.Item> patternPartResults = executePatternPartConstraint(ctx, patternPartMatchConstraint, scope);
            if (results != null) {
                results = VertexiumCypherScope.Item.cartesianProduct(results, patternPartResults);
            } else {
                results = patternPartResults;
            }
        }

        for (CypherAstBase whereExpression : matchConstraints.getWhereExpressions()) {
            results = ctx.getExpressionExecutor().applyWhereToResults(ctx, results, whereExpression);
        }
        return results;
    }

    public Stream<VertexiumCypherScope.Item> executePatternPartConstraint(
        VertexiumCypherQueryContext ctx,
        PatternPartMatchConstraint patternPartConstraint,
        ExpressionScope scope
    ) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new MatchContextIterator(ctx, patternPartConstraint, scope), Spliterator.IMMUTABLE), false)
            .map(mc -> mc.toResult(patternPartConstraint.getNamedPaths(), scope));
    }

    private class MatchContextIterator implements Iterator<MatchContext> {
        private final VertexiumCypherQueryContext ctx;
        private final ExpressionScope scope;
        private final LinkedList<Iterator<MatchContext>> matchContextsQueue = new LinkedList<>();
        private MatchContext next;
        private MatchContext current;

        public MatchContextIterator(
            VertexiumCypherQueryContext ctx,
            PatternPartMatchConstraint patternPartConstraint,
            ExpressionScope scope
        ) {
            this.ctx = ctx;
            this.scope = scope;
            matchContextsQueue.add(getInitialMatchContexts(ctx, patternPartConstraint, scope).iterator());
        }

        @Override
        public boolean hasNext() {
            loadNext();
            return next != null;
        }

        @Override
        public MatchContext next() {
            loadNext();
            this.current = this.next;
            this.next = null;
            return this.current;
        }

        private void loadNext() {
            if (this.next != null) {
                return;
            }

            while (matchContextsQueue.size() > 0) {
                Iterator<MatchContext> mcs = matchContextsQueue.peek();
                if (!mcs.hasNext()) {
                    matchContextsQueue.remove();
                    continue;
                }
                MatchContext mc = mcs.next();
                if (mc.isDone()) {
                    this.next = mc;
                    return;
                }
                matchContextsQueue.addFirst(resolveMatchContext(ctx, mc, scope).iterator());
            }
        }
    }

    private Stream<MatchContext> getInitialMatchContexts(VertexiumCypherQueryContext ctx, PatternPartMatchConstraint patternPartConstraint, ExpressionScope scope) {
        List<MatchConstraint> foundMatchConstraints = findExistingMatchedMatchConstraintInScope(patternPartConstraint, scope);
        if (foundMatchConstraints.size() > 0) {
            return getInitialMatchContextsFromFoundItems(patternPartConstraint, foundMatchConstraints, scope);
        }
        return getInitialMatchContextsBySearching(ctx, patternPartConstraint, scope);
    }

    private Stream<MatchContext> getInitialMatchContextsBySearching(VertexiumCypherQueryContext ctx, PatternPartMatchConstraint patternPartConstraint, ExpressionScope scope) {
        MatchConstraint workingMatchConstraint = MatchContext.getNextConstraintToWorkOn(ctx, scope, patternPartConstraint.getMatchConstraints(), new HashMap<>());
        LOGGER.debug("working on: %s", workingMatchConstraint);
        Stream<? extends Element> matchingElements = executeFirstMatchConstraint(ctx, workingMatchConstraint, scope, null).getHits();
        if (workingMatchConstraint.isOptional()) {
            matchingElements = StreamUtils.ifEmpty(matchingElements, () -> Stream.of((Element) null), s -> s);
        }
        return matchingElements
            .map(element -> {
                List<MatchConstraint> remainingMatchConstraints = new ArrayList<>(patternPartConstraint.getMatchConstraints());
                remainingMatchConstraints.remove(workingMatchConstraint);
                return new MatchContext(workingMatchConstraint, element, remainingMatchConstraints);
            });
    }

    private Stream<MatchContext> getInitialMatchContextsFromFoundItems(
        PatternPartMatchConstraint patternPartConstraint,
        List<MatchConstraint> foundMatchConstraints,
        ExpressionScope scope
    ) {
        List<MatchContext> matchContexts = new ArrayList<>();
        for (MatchConstraint foundMatchConstraint : foundMatchConstraints) {
            Object objByName = scope.getByName(foundMatchConstraint.getName());
            appendMatchContextsWithFoundItems(matchContexts, patternPartConstraint, foundMatchConstraint, objByName);
        }
        return matchContexts.stream();
    }

    private void appendMatchContextsWithFoundItems(
        List<MatchContext> matchContexts,
        PatternPartMatchConstraint patternPartConstraint,
        MatchConstraint foundMatchConstraint,
        Object objByName
    ) {
        if (objByName == null || objByName instanceof Element) {
            Element element = (Element) objByName;
            if (matchContexts.size() == 0) {
                List<MatchConstraint> remainingMatchConstraints = new ArrayList<>(patternPartConstraint.getMatchConstraints());
                remainingMatchConstraints.remove(foundMatchConstraint);
                matchContexts.add(new MatchContext(foundMatchConstraint, element, remainingMatchConstraints));
            } else {
                for (MatchContext matchContext : matchContexts) {
                    matchContext.addElement(foundMatchConstraint, element);
                }
            }
        } else if (objByName instanceof Stream) {
            ((Stream<?>) objByName).forEach(o -> appendMatchContextsWithFoundItems(matchContexts, patternPartConstraint, foundMatchConstraint, o));
        } else if (objByName instanceof List) {
            for (Object o : ((List<?>) objByName)) {
                appendMatchContextsWithFoundItems(matchContexts, patternPartConstraint, foundMatchConstraint, o);
            }
        } else {
            throw new VertexiumCypherNotImplemented("non-objects: " + objByName);
        }
    }

    private List<MatchConstraint> findExistingMatchedMatchConstraintInScope(PatternPartMatchConstraint patternPartConstraint, ExpressionScope scope) {
        List<MatchConstraint> results = new ArrayList<>();
        for (MatchConstraint matchConstraint : patternPartConstraint.getMatchConstraints()) {
            String name = matchConstraint.getName();
            if (name != null) {
                if (scope.contains(name)) {
                    Object obj = scope.getByName(name);
                    if (obj instanceof Stream) {
                        obj = ((Stream<?>) obj).collect(Collectors.toList());
                    }
                    if (obj instanceof List && ((List) obj).size() == 0) {
                        continue;
                    }
                    results.add(matchConstraint);
                }
            }
        }
        return results;
    }

    private Stream<MatchContext> resolveMatchContext(
        VertexiumCypherQueryContext ctx,
        MatchContext matchContext,
        ExpressionScope scope
    ) {
        MatchConstraint<?, ?> matchConstraint = matchContext.getNextConstraintToWorkOn(ctx, scope);
        LOGGER.trace("working on: %s", matchConstraint);
        if (matchConstraint == null) {
            throw new VertexiumCypherException("Cannot solve match clause. Could not find and constraints to work on.");
        } else if (matchConstraint instanceof NodeMatchConstraint) {
            return resolveNodeMatchContext(ctx, matchContext, (NodeMatchConstraint) matchConstraint, scope);
        } else if (matchConstraint instanceof RelationshipMatchConstraint) {
            return resolveRelationshipMatchContext(ctx, matchContext, (RelationshipMatchConstraint) matchConstraint, scope);
        } else {
            throw new VertexiumCypherTypeErrorException(matchConstraint, NodeMatchConstraint.class, RelationshipMatchConstraint.class);
        }
    }

    private Stream<MatchContext> resolveRelationshipMatchContext(
        VertexiumCypherQueryContext ctx,
        MatchContext matchContext,
        RelationshipMatchConstraint relationshipMatchConstraint,
        ExpressionScope scope
    ) {
        List<Vertex> previousVertices = matchContext.getMatchedVertices(relationshipMatchConstraint);
        if (previousVertices.size() > 2) {
            throw new VertexiumCypherNotImplemented("Too many vertices");
        }
        Vertex startingVertex = previousVertices.size() > 0 ? previousVertices.get(0) : null;
        Vertex endVertex = previousVertices.size() > 1 ? previousVertices.get(1) : null;
        Stream<VertexiumCypherScope.PathItem> paths = executeRelationshipConstraint(
            ctx,
            startingVertex,
            endVertex,
            relationshipMatchConstraint,
            matchContext,
            scope
        );
        return paths
            .map(path -> MatchContext.concatPath(relationshipMatchConstraint, matchContext, path));
    }

    private Stream<MatchContext> resolveNodeMatchContext(
        VertexiumCypherQueryContext ctx,
        MatchContext matchContext,
        NodeMatchConstraint nodeMatchConstraint,
        ExpressionScope scope
    ) {
        List<EdgeVertexConstraint> satisfiedEdgeVertexConstraint = matchContext.getSatisfiedEdgeVertexPairs(ctx, nodeMatchConstraint);
        if (satisfiedEdgeVertexConstraint.size() == 0) {
            return Stream.empty();
        }
        Stream<Vertex> vertices = executeNodeConstraints(
            ctx,
            matchContext,
            satisfiedEdgeVertexConstraint,
            nodeMatchConstraint,
            scope
        );
        return vertices
            .map(v -> MatchContext.concatVertex(nodeMatchConstraint, matchContext, v));
    }

    private Stream<Vertex> executeNodeConstraints(
        VertexiumCypherQueryContext ctx,
        MatchContext matchContext,
        List<EdgeVertexConstraint> edgeVertexConstraints,
        NodeMatchConstraint matchConstraint,
        ExpressionScope scope
    ) {
        if (edgeVertexConstraints.size() == 0) {
            throw new VertexiumCypherException("no edge vertex constraints found");
        }

        List<String> labelNames = getLabelNamesFromMatchConstraint(matchConstraint).stream()
            .map(ctx::normalizeLabelName)
            .collect(Collectors.toList());
        ListMultimap<String, CypherAstBase> propertiesMap = getPropertiesMapFromElementPatterns(ctx, matchConstraint.getPatterns());

        Stream<Vertex> results = Stream.empty();
        for (EdgeVertexConstraint edgeVertexConstraint : edgeVertexConstraints) {
            Stream<Vertex> newResults = executeNodeConstraint(
                ctx,
                matchContext,
                matchConstraint,
                edgeVertexConstraint,
                labelNames,
                propertiesMap,
                scope
            );
            results = Stream.concat(results, newResults);
        }
        return results.distinct();
    }

    private Stream<Vertex> executeNodeConstraint(
        VertexiumCypherQueryContext ctx,
        MatchContext matchContext,
        NodeMatchConstraint matchConstraint,
        EdgeVertexConstraint edgeVertexConstraint,
        List<String> labelNames,
        ListMultimap<String, CypherAstBase> propertiesMap,
        ExpressionScope scope
    ) {
        List<Vertex> results = new ArrayList<>();
        Edge edge = edgeVertexConstraint.getEdge();
        Vertex previousVertex = edgeVertexConstraint.getVertex();
        MatchConstraint edgeMatchConstraint = edgeVertexConstraint.getEdgeMatchConstraint();
        boolean foundMatch = false;
        if (edge != null && previousVertex != null) {
            Vertex vertex = edge.getOtherVertex(previousVertex.getId(), ctx.getFetchHints(), ctx.getUser());
            if (vertexIsMatch(ctx, vertex, labelNames, propertiesMap, scope)
                && vertexRelationshipMatches(matchContext, matchConstraint, vertex)) {
                results.add(vertex);
                foundMatch = true;
            }
        }
        if (!foundMatch && edge == null && previousVertex != null && edgeMatchConstraint.hasZeroRangePattern()) {
            results.add(previousVertex);
            foundMatch = true;
        }
        if (!foundMatch && matchConstraint.isOptional()) {
            results.add(null);
        }
        return results.stream();
    }

    private boolean vertexRelationshipMatches(MatchContext matchContext, NodeMatchConstraint matchConstraint, Vertex vertex) {
        for (RelationshipMatchConstraint relationshipMatchConstraint : matchConstraint.getConnectedConstraints()) {
            Object o = matchContext.getResultsByMatchConstraint(relationshipMatchConstraint);
            if (o != null) {
                if (o instanceof Edge) {
                    Edge edge = (Edge) o;
                    if (!edge.getVertexId(Direction.OUT).equals(vertex.getId())
                        && !edge.getVertexId(Direction.IN).equals(vertex.getId())) {
                        return false;
                    }
                } else if (o instanceof VertexiumCypherScope.PathItem) {
                    VertexiumCypherScope.PathItem pathItem = (VertexiumCypherScope.PathItem) o;
                    if (!pathItem.canVertexConnectOrFoundAtStartOrEnd(vertex)) {
                        return false;
                    }
                } else {
                    throw new VertexiumCypherException("Unexpected result type: " + o.getClass().getName());
                }
            }
        }
        return true;
    }

    private boolean vertexIsMatch(
        VertexiumCypherQueryContext ctx,
        Vertex vertex,
        List<String> labelNames,
        ListMultimap<String, CypherAstBase> propertiesMap,
        ExpressionScope scope
    ) {
        Set<String> vertexLabelNames = ctx.getVertexLabels(vertex);
        for (String labelName : labelNames) {
            if (!vertexLabelNames.contains(labelName)) {
                return false;
            }
        }

        return propertyMapMatch(ctx, vertex, propertiesMap, scope);
    }

    private static long getTotalHits(
        VertexiumCypherQueryContext ctx,
        MatchConstraint<?, ?> matchConstraint,
        ExpressionScope scope
    ) {
        return ctx.getTotalHitsForMatchConstraint(
            matchConstraint,
            mc -> executeFirstMatchConstraint(ctx, mc, scope, 0L).getTotalHits()
        );
    }

    private static QueryResults<? extends Element> executeFirstMatchConstraint(
        VertexiumCypherQueryContext ctx,
        MatchConstraint<?, ?> matchConstraint,
        ExpressionScope scope,
        Long limit
    ) {
        try {
            List<String> labelNames = getLabelNamesFromMatchConstraint(matchConstraint);
            ListMultimap<String, CypherAstBase> propertiesMap = getPropertiesMapFromElementPatterns(ctx, matchConstraint.getPatterns());
            QueryResults<? extends Element> elements;
            org.vertexium.search.Query query = ctx.getGraph().query(ctx.getUser())
                .limit(limit);
            if (labelNames.size() == 0 && propertiesMap.size() == 0) {
                elements = executeQuery(ctx, query, matchConstraint);
            } else {
                if (labelNames.size() > 0) {
                    Stream<String> labelNamesStream = labelNames.stream()
                        .map(ctx::normalizeLabelName);

                    if (matchConstraint instanceof NodeMatchConstraint) {
                        query = labelNamesStream
                            .reduce(
                                query,
                                (q, labelName) -> q.has(ctx.getLabelPropertyName(), labelName),
                                (q, q2) -> q
                            );
                    } else if (matchConstraint instanceof RelationshipMatchConstraint) {
                        List<String> normalizedLabelNames = labelNamesStream.collect(Collectors.toList());
                        query = query.hasEdgeLabel(normalizedLabelNames);
                    } else {
                        throw new VertexiumCypherNotImplemented("unexpected constraint type: " + matchConstraint.getClass().getName());
                    }
                }

                for (Map.Entry<String, CypherAstBase> propertyMatch : propertiesMap.entries()) {
                    Object value = ctx.getExpressionExecutor().executeExpression(ctx, propertyMatch.getValue(), scope);
                    if (value instanceof CypherAstBase) {
                        throw new VertexiumException("unexpected value: " + value.getClass().getName() + ": " + value);
                    }
                    if (value instanceof Stream) {
                        value = ((Stream<?>) value).collect(Collectors.toList());
                    }
                    if (value instanceof Collection) {
                        query.has(propertyMatch.getKey(), Contains.IN, value);
                    } else {
                        query.has(propertyMatch.getKey(), value);
                    }
                }

                elements = executeQuery(ctx, query, matchConstraint);
            }

            return elements;
        } catch (VertexiumPropertyNotDefinedException e) {
            LOGGER.error(e.getMessage());
            return new EmptyQueryResults<>();
        }
    }

    private static QueryResults<? extends Element> executeQuery(
        VertexiumCypherQueryContext ctx,
        org.vertexium.search.Query query,
        MatchConstraint<?, ?> matchConstraint
    ) {
        QueryResults<? extends Element> elements;
        if (matchConstraint instanceof NodeMatchConstraint) {
            elements = query.vertices(ctx.getFetchHints());
        } else if (matchConstraint instanceof RelationshipMatchConstraint) {
            elements = query.edges(ctx.getFetchHints());
        } else {
            throw new VertexiumCypherNotImplemented("unexpected constraint type: " + matchConstraint.getClass().getName());
        }
        return elements;
    }

    private Stream<VertexiumCypherScope.PathItem> executeRelationshipConstraint(
        VertexiumCypherQueryContext ctx,
        Vertex startingVertex,
        Vertex endVertex,
        RelationshipMatchConstraint matchConstraint,
        MatchContext matchContext,
        ExpressionScope scope
    ) {
        List<String> labelNames = getRelationshipTypeNamesFromMatchConstraint(matchConstraint).stream()
            .map(ctx::normalizeLabelName)
            .collect(Collectors.toList());
        ListMultimap<String, CypherAstBase> propertiesMap = getPropertiesMapFromElementPatterns(ctx, matchConstraint.getPatterns());
        Direction direction = matchContext.getRelationshipDirection(matchConstraint, startingVertex, endVertex);
        RelationshipMatchRange range = matchConstraint.getRange();
        String name = matchConstraint.getName();

        Stream<VertexiumCypherScope.PathItem> newPaths = Stream.empty();
        VertexiumCypherScope.PathItem previousPath = VertexiumCypherScope.newEmptyPathItem(null, scope)
            .setPrintMode(VertexiumCypherScope.PathItem.PrintMode.RELATIONSHIP_RANGE);
        previousPath = previousPath.concat(null, startingVertex);
        Vertex vertex = (Vertex) previousPath.getLastElement();
        if (range.isRangeSet() && range.isIn(0)) {
            newPaths = Stream.concat(newPaths, Stream.of(previousPath.concat(name, null)));
        }
        Stream<VertexiumCypherScope.PathItem> newPathsToAdd = findPathsToAdd(
            ctx,
            previousPath,
            vertex,
            endVertex,
            name,
            matchConstraint.isOptional(),
            range,
            1,
            labelNames,
            propertiesMap,
            direction,
            matchContext,
            scope
        );
        newPaths = Stream.concat(newPaths, newPathsToAdd);
        return newPaths;
    }

    private Stream<VertexiumCypherScope.PathItem> findPathsToAdd(
        VertexiumCypherQueryContext ctx,
        VertexiumCypherScope.PathItem previousPath,
        Vertex startingVertex,
        Vertex endVertex,
        String name,
        boolean optional,
        RelationshipMatchRange range,
        int depth,
        List<String> labelNames,
        ListMultimap<String, CypherAstBase> propertiesMap,
        Direction direction,
        MatchContext matchContext,
        ExpressionScope scope
    ) {
        if (range.isRangeSet() && range.getTo() == null && depth > ctx.getMaxUnboundedRange()) {
            return Stream.empty();
        }
        if (startingVertex == null && depth > 1) {
            return Stream.empty();
        }

        AtomicReference<Stream<VertexiumCypherScope.PathItem>> paths = new AtomicReference<>(Stream.empty());

        if (range.isIn(depth)) {
            AtomicBoolean foundEdge = new AtomicBoolean(false);
            if (startingVertex != null) {
                findEdges(ctx, name, propertiesMap, startingVertex, direction, labelNames)
                    .forEach(edge -> {
                        if (previousPath.contains(edge) || matchContext.contains(edge)) {
                            return;
                        }
                        if (endVertex != null) {
                            if (!edge.getOtherVertexId(startingVertex.getId()).equals(endVertex.getId())) {
                                return;
                            }
                        }
                        if (edgeIsMatch(ctx, edge, labelNames, propertiesMap, scope)) {
                            paths.set(Stream.concat(paths.get(), Stream.of(previousPath.concat(name, edge))));
                            foundEdge.set(true);
                        }
                    });
            }
            if (optional && !foundEdge.get()) {
                paths.set(Stream.concat(paths.get(), Stream.of(previousPath.concat(name, null))));
            }
        }

        if (range.isRangeSet()) {
            if (startingVertex != null) {
                findEdges(ctx, name, propertiesMap, startingVertex, direction, labelNames)
                    .forEach(edge -> {
                        if (previousPath.contains(edge) || matchContext.contains(edge)) {
                            return;
                        }
                        if (edgeIsMatch(ctx, edge, labelNames, propertiesMap, scope)) {
                            Vertex otherVertex = edge.getOtherVertex(startingVertex.getId(), ctx.getFetchHints(), ctx.getUser());
                            VertexiumCypherScope.PathItem newPath = previousPath
                                .concat(name, edge)
                                .concat(null, otherVertex);
                            paths.set(Stream.concat(paths.get(), findPathsToAdd(
                                ctx,
                                newPath,
                                otherVertex,
                                endVertex,
                                name,
                                optional,
                                range,
                                depth + 1,
                                labelNames,
                                propertiesMap,
                                direction,
                                matchContext, scope
                            )));
                        }
                    });
            }
            if (optional) {
                VertexiumCypherScope.PathItem newPath = previousPath
                    .concat(name, null)
                    .concat(null, null);
                paths.set(Stream.concat(paths.get(), findPathsToAdd(
                    ctx,
                    newPath,
                    null,
                    endVertex, name,
                    optional,
                    range,
                    depth + 1,
                    labelNames,
                    propertiesMap,
                    direction,
                    matchContext, scope
                )));
            }
        }

        return paths.get();
    }

    private Stream<Edge> findEdges(
        VertexiumCypherQueryContext ctx,
        String name,
        ListMultimap<String, CypherAstBase> propertiesMap,
        Vertex startingVertex,
        Direction direction,
        List<String> labelNames
    ) {
        if (name == null && propertiesMap.size() == 0) {
            return startingVertex.getEdgeInfos(direction, labelNamesToArray(labelNames), ctx.getUser())
                .map(edgeInfo -> new EdgeInfoEdge(ctx.getGraph(), startingVertex.getId(), edgeInfo, ctx.getFetchHints(), ctx.getUser()));
        }
        return startingVertex.getEdges(direction, labelNamesToArray(labelNames), ctx.getFetchHints(), ctx.getUser());
    }

    private String[] labelNamesToArray(List<String> labelNames) {
        if (labelNames == null || labelNames.size() == 0) {
            return null;
        }
        return labelNames.toArray(new String[labelNames.size()]);
    }

    private boolean edgeIsMatch(
        VertexiumCypherQueryContext ctx,
        Edge edge,
        List<String> labelNames,
        ListMultimap<String, CypherAstBase> propertiesMap,
        ExpressionScope scope
    ) {
        if (labelNames.size() > 0) {
            if (labelNames.stream().noneMatch(ln -> edge.getLabel().equals(ln))) {
                return false;
            }
        }

        return propertyMapMatch(ctx, edge, propertiesMap, scope);
    }

    private boolean propertyMapMatch(
        VertexiumCypherQueryContext ctx,
        Element element,
        ListMultimap<String, CypherAstBase> propertiesMap,
        ExpressionScope scope
    ) {
        for (Map.Entry<String, CypherAstBase> propertyEntry : propertiesMap.entries()) {
            Object propertyValue = element.getPropertyValue(propertyEntry.getKey());
            Object expressionValue = ctx.getExpressionExecutor().executeExpression(ctx, propertyEntry.getValue(), scope);
            if (!ObjectUtils.equals(propertyValue, expressionValue)) {
                return false;
            }
        }
        return true;
    }

    private List<String> getRelationshipTypeNamesFromMatchConstraint(RelationshipMatchConstraint matchConstraint) {
        List<String> results = new ArrayList<>();
        for (CypherRelationshipPattern relationshipPattern : matchConstraint.getPatterns()) {
            if (relationshipPattern.getRelTypeNames() != null) {
                for (CypherRelTypeName relTypeName : relationshipPattern.getRelTypeNames()) {
                    results.add(relTypeName.getValue());
                }
            }
        }
        return results;
    }

    private static List<String> getLabelNamesFromMatchConstraint(MatchConstraint<?, ?> matchConstraint) {
        List<String> results = new ArrayList<>();
        for (CypherElementPattern pattern : matchConstraint.getPatterns()) {
            if (pattern instanceof CypherNodePattern) {
                CypherNodePattern nodePattern = (CypherNodePattern) pattern;
                if (nodePattern.getLabelNames() != null) {
                    for (CypherLabelName labelName : nodePattern.getLabelNames()) {
                        results.add(labelName.getValue());
                    }
                }
            } else if (pattern instanceof CypherRelationshipPattern) {
                CypherRelationshipPattern relationshipPattern = (CypherRelationshipPattern) pattern;
                if (relationshipPattern.getRelTypeNames() != null) {
                    for (CypherRelTypeName relTypeName : relationshipPattern.getRelTypeNames()) {
                        results.add(relTypeName.getValue());
                    }
                }
            } else {
                throw new VertexiumCypherNotImplemented("unexpected pattern type: " + pattern.getClass().getName());
            }
        }
        return results;
    }

    private static <T extends CypherElementPattern> ListMultimap<String, CypherAstBase> getPropertiesMapFromElementPatterns(
        VertexiumCypherQueryContext ctx,
        List<T> elementPatterns
    ) {
        ListMultimap<String, CypherAstBase> results = ArrayListMultimap.create();
        for (CypherElementPattern elementPattern : elementPatterns) {
            for (Map.Entry<String, CypherAstBase> entry : elementPattern.getPropertiesMap().entrySet()) {
                results.put(ctx.normalizePropertyName(entry.getKey()), entry.getValue());
            }
        }
        return results;
    }

    private static class EdgeVertexConstraint {
        private Edge edge;
        private Vertex vertex;
        private final MatchConstraint edgeMatchConstraint;

        public EdgeVertexConstraint(Edge edge, Vertex vertex, MatchConstraint edgeMatchConstraint) {
            this.edge = edge;
            this.vertex = vertex;
            this.edgeMatchConstraint = edgeMatchConstraint;
        }

        public Edge getEdge() {
            return edge;
        }

        public Vertex getVertex() {
            return vertex;
        }

        public MatchConstraint getEdgeMatchConstraint() {
            return edgeMatchConstraint;
        }
    }

    private static class MatchContext {
        public Map<MatchConstraint, Object> elementsByMatchConstraint = new HashMap<>();
        public LinkedHashMap<String, Object> elementsByName = new LinkedHashMap<>();
        public List<MatchConstraint> remainingMatchConstraints = new ArrayList<>();

        private MatchContext() {

        }

        public MatchContext(MatchConstraint matchConstraint, Object element, List<MatchConstraint> remainingMatchConstraints) {
            this.elementsByMatchConstraint.put(matchConstraint, element);
            this.remainingMatchConstraints.addAll(remainingMatchConstraints);
            if (matchConstraint.getName() != null) {
                this.elementsByName.put(matchConstraint.getName(), element);
            }
        }

        public static MatchContext concatVertex(MatchConstraint matchConstraint, MatchContext previousMatchContext, Vertex vertex) {
            return concatItem(matchConstraint, previousMatchContext, vertex);
        }

        public static MatchContext concatPath(MatchConstraint matchConstraint, MatchContext previousMatchContext, VertexiumCypherScope.PathItem path) {
            return concatItem(matchConstraint, previousMatchContext, path);
        }

        private static MatchContext concatItem(MatchConstraint matchConstraint, MatchContext previousMatchContext, Object o) {
            MatchContext ctx = new MatchContext();
            ctx.elementsByMatchConstraint.put(matchConstraint, o);
            ctx.remainingMatchConstraints.addAll(previousMatchContext.remainingMatchConstraints);
            ctx.remainingMatchConstraints.remove(matchConstraint);
            ctx.elementsByMatchConstraint.putAll(previousMatchContext.elementsByMatchConstraint);
            ctx.elementsByName.putAll(previousMatchContext.elementsByName);
            if (matchConstraint.getName() != null) {
                ctx.elementsByName.put(matchConstraint.getName(), o);
            }
            return ctx;
        }

        public void addElement(MatchConstraint matchConstraint, Element element) {
            if (element == null && !matchConstraint.isOptional()) {
                throw new VertexiumCypherException("element cannot be null");
            }
            elementsByMatchConstraint.put(matchConstraint, element);
            remainingMatchConstraints.remove(matchConstraint);
            if (matchConstraint.getName() != null) {
                elementsByName.put(matchConstraint.getName(), element);
            }
        }

        public boolean isDone() {
            return remainingMatchConstraints.size() == 0;
        }

        public MatchConstraint<?, ?> getNextConstraintToWorkOn(
            VertexiumCypherQueryContext ctx,
            ExpressionScope scope
        ) {
            Map<MatchConstraint, Integer> satisfiedConstraintCount = remainingMatchConstraints.stream()
                .collect(Collectors.toMap(c -> c, this::getSatisfiedConstraintCount));

            return getNextConstraintToWorkOn(ctx, scope, remainingMatchConstraints, satisfiedConstraintCount);
        }

        private static MatchConstraint getNextConstraintToWorkOn(
            VertexiumCypherQueryContext ctx,
            ExpressionScope scope,
            Collection<MatchConstraint> remainingMatchConstraints,
            Map<MatchConstraint, Integer> satisfiedConstraintCount
        ) {
            return remainingMatchConstraints.stream()
                .sorted((o1, o2) -> {
                    if (satisfiedConstraintCount.size() > 0) {
                        int o1SatisfiedCount = satisfiedConstraintCount.getOrDefault(o1, 0);
                        int o2SatisfiedCount = satisfiedConstraintCount.getOrDefault(o2, 0);
                        int o1UnsatisfiedCount = o1.getConnectedConstraints().size() - o1SatisfiedCount;
                        int o2UnsatisfiedCount = o2.getConnectedConstraints().size() - o2SatisfiedCount;

                        // pick most satisfied
                        int result = Integer.compare(o1SatisfiedCount, o2SatisfiedCount);
                        if (result != 0) {
                            return -result;
                        }

                        // pick least unsatisfied
                        result = Integer.compare(o1UnsatisfiedCount, o2UnsatisfiedCount);
                        if (result != 0) {
                            return result;
                        }
                    }

                    // pick the non-optional one first
                    if (o1.isOptional() || o2.isOptional()) {
                        if (o1.isOptional() && o2.isOptional()) {
                            return 0;
                        }
                        if (o1.isOptional()) {
                            return 1;
                        }
                        if (o2.isOptional()) {
                            return -1;
                        }
                    }

                    // pick the relationship without a range set
                    if (o1 instanceof RelationshipMatchConstraint && ((RelationshipMatchConstraint) o1).getRange().isRangeSet()) {
                        return 1;
                    }
                    if (o2 instanceof RelationshipMatchConstraint && ((RelationshipMatchConstraint) o2).getRange().isRangeSet()) {
                        return -1;
                    }

                    long o1Hits = getTotalHits(ctx, o1, scope);
                    long o2Hits = getTotalHits(ctx, o2, scope);
                    int i = Long.compare(o1Hits, o2Hits);
                    if (i != 0) {
                        return i;
                    }

                    // find the one most constrained
                    return -Integer.compare(o1.getConstraintCount(), o2.getConstraintCount());
                })
                .findFirst()
                .orElse(null);
        }

        private int getSatisfiedConstraintCount(MatchConstraint<?, ?> remainingMatchConstraint) {
            return (int) remainingMatchConstraint.getConnectedConstraints().stream()
                .filter(c -> elementsByMatchConstraint.containsKey(c))
                .count();
        }

        public List<Vertex> getMatchedVertices(RelationshipMatchConstraint matchConstraint) {
            return matchConstraint.getConnectedConstraints().stream()
                .map(mc -> (Vertex) elementsByMatchConstraint.get(mc))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }

        public List<EdgeVertexConstraint> getSatisfiedEdgeVertexPairs(VertexiumCypherQueryContext ctx, NodeMatchConstraint matchConstraint) {
            return matchConstraint.getConnectedConstraints().stream()
                .flatMap(edgeMatchConstraint -> {
                    Object o = elementsByMatchConstraint.get(edgeMatchConstraint);
                    Edge edge;
                    if (o == null) {
                        return null;
                    } else if (o instanceof Edge) {
                        edge = (Edge) o;
                    } else if (o instanceof VertexiumCypherScope.PathItem) {
                        VertexiumCypherScope.PathItem pathResult = (VertexiumCypherScope.PathItem) o;
                        edge = (Edge) pathResult.getLastElement();
                        Vertex vertex = (Vertex) pathResult.getElement(-2);
                        return Lists.newArrayList(
                            new EdgeVertexConstraint(edge, vertex, edgeMatchConstraint)
                        ).stream();
                    } else {
                        throw new VertexiumCypherTypeErrorException(o, Edge.class, VertexiumCypherScope.PathItem.class, null);
                    }
                    List<Vertex> matchedVertices = getMatchedVertices(edgeMatchConstraint);

                    // this can happen if we start the search with relationships
                    if (matchedVertices.size() == 0) {
                        EdgeVertices edgeVertices = edge.getVertices(ctx.getFetchHints(), ctx.getUser());
                        switch (edgeMatchConstraint.getDirection()) {
                            case BOTH:
                            case UNSPECIFIED:
                                matchedVertices.add(edgeVertices.getInVertex());
                                matchedVertices.add(edgeVertices.getOutVertex());
                                break;
                            case OUT:
                                if (edgeMatchConstraint.isFoundInNext(matchConstraint)) {
                                    matchedVertices.add(edgeVertices.getOutVertex());
                                } else if (edgeMatchConstraint.isFoundInPrevious(matchConstraint)) {
                                    matchedVertices.add(edgeVertices.getInVertex());
                                } else {
                                    throw new VertexiumCypherException("unexpected");
                                }
                                break;
                            case IN:
                                if (edgeMatchConstraint.isFoundInPrevious(matchConstraint)) {
                                    matchedVertices.add(edgeVertices.getOutVertex());
                                } else if (edgeMatchConstraint.isFoundInNext(matchConstraint)) {
                                    matchedVertices.add(edgeVertices.getInVertex());
                                } else {
                                    throw new VertexiumCypherException("unexpected");
                                }
                                break;
                        }
                    }

                    return matchedVertices.stream()
                        .filter(Objects::nonNull)
                        .map(v -> new EdgeVertexConstraint(edge, v, edgeMatchConstraint));
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }

        public VertexiumCypherScope.Item toResult(Map<String, List<MatchConstraint>> namedPaths, ExpressionScope parentScope) {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : elementsByName.entrySet()) {
                String name = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof VertexiumCypherScope.PathItem) {
                    VertexiumCypherScope.PathItem pr = (VertexiumCypherScope.PathItem) value;
                    List<Edge> edges = pr.getEdges();
                    if (edges.size() == 0) {
                        value = null;
                    } else if (edges.size() == 1) {
                        value = edges.get(0);
                    }
                }
                values.put(name, value);
            }

            for (Map.Entry<String, List<MatchConstraint>> namedPath : namedPaths.entrySet()) {
                String pathName = namedPath.getKey();
                VertexiumCypherScope.PathItem path = toPathResult(pathName, namedPath.getValue(), parentScope);
                values.put(pathName, path);
            }

            return VertexiumCypherScope.newMapItem(values, parentScope);
        }

        private VertexiumCypherScope.PathItem toPathResult(String pathName, List<MatchConstraint> matchConstraints, ExpressionScope parentScope) {
            VertexiumCypherScope.PathItem result = VertexiumCypherScope.newEmptyPathItem(pathName, parentScope);
            for (MatchConstraint matchConstraint : matchConstraints) {
                String elementName = matchConstraint.getName();
                Object o = elementsByMatchConstraint.get(matchConstraint);
                if (o == null) {
                    return null;
                } else if (o instanceof Element) {
                    result = result.concat(elementName, (Element) o);
                } else if (o instanceof VertexiumCypherScope.PathItem) {
                    result = result.concat((VertexiumCypherScope.PathItem) o);
                } else {
                    throw new VertexiumCypherTypeErrorException(o, Element.class, VertexiumCypherScope.PathItem.class, null);
                }
            }
            return result;
        }

        public boolean contains(Edge edge) {
            for (Map.Entry<MatchConstraint, Object> entry : this.elementsByMatchConstraint.entrySet()) {
                Object o = entry.getValue();
                if (o == null) {
                    // not an edge
                } else if (o instanceof Vertex) {
                    // not an edge
                } else if (o instanceof Edge) {
                    if (edge.equals(o)) {
                        return true;
                    }
                } else if (o instanceof VertexiumCypherScope.PathItem) {
                    if (((VertexiumCypherScope.PathItem) o).contains(edge)) {
                        return true;
                    }
                } else {
                    throw new VertexiumCypherNotImplemented("unknown object type: " + o.getClass().getName());
                }
            }
            return false;
        }

        public Direction getRelationshipDirection(RelationshipMatchConstraint matchConstraint, Vertex startingVertex, Vertex endVertex) {
            Direction direction = matchConstraint.getDirection().toVertexiumDirection();
            if (direction == Direction.BOTH) {
                return direction;
            }

            if (startingVertex == null && endVertex == null) {
                return null;
            }

            if (startingVertex != null) {
                Direction d = findConstraintsByElement(startingVertex)
                    .map(startingMatchConstraint -> {
                        NodeMatchConstraint nodeMatchConstraint = (NodeMatchConstraint) startingMatchConstraint;
                        if (matchConstraint.isFoundInPrevious(nodeMatchConstraint)) {
                            return direction;
                        } else if (matchConstraint.isFoundInNext(nodeMatchConstraint)) {
                            return direction.reverse();
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
                if (d == null) {
                    throw new VertexiumCypherException("Could not find starting match constraint in next or previous");
                }
                return d;
            }

            throw new VertexiumCypherNotImplemented("getRelationshipDirection by end vertex");
        }

        private Stream<? extends MatchConstraint> findConstraintsByElement(Element element) {
            return elementsByMatchConstraint.entrySet().stream()
                .filter(e -> element.equals(e.getValue()))
                .map(Map.Entry::getKey);
        }

        public Object getResultsByMatchConstraint(RelationshipMatchConstraint relationshipMatchConstraint) {
            return elementsByMatchConstraint.get(relationshipMatchConstraint);
        }
    }
}
