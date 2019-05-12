package org.vertexium.cypher.executionPlan;

import org.vertexium.Direction;
import org.vertexium.Edge;
import org.vertexium.Element;
import org.vertexium.Vertex;
import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.PathResultBase;
import org.vertexium.cypher.RelationshipRangePathResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherDirection;
import org.vertexium.cypher.ast.model.CypherRangeLiteral;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.search.Query;
import org.vertexium.search.QueryResults;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.mapOptional;

public class MatchRelationshipPartExecutionStep extends MatchPartExecutionStep<MatchNodePartExecutionStep> {
    private final List<String> relTypesNames;
    private final CypherDirection direction;
    private final CypherRangeLiteral range;

    public MatchRelationshipPartExecutionStep(
        String originalName,
        String resultName,
        boolean optional,
        List<String> relTypesNames,
        CypherDirection direction,
        CypherRangeLiteral range,
        List<ExecutionStepWithResultName> properties
    ) {
        super(originalName, resultName, optional, properties);
        this.relTypesNames = relTypesNames;
        this.direction = direction;
        this.range = range;
    }

    @Override
    protected QueryResults<? extends Element> getElements(VertexiumCypherQueryContext ctx, Query q) {
        QueryResults<Edge> elements;
        if (relTypesNames.size() > 0) {
            throw new VertexiumCypherNotImplemented("cannot filter rel types names");
        }
        elements = q.edges(ctx.getFetchHints()); // TODO calculate best fetch hints
        return elements;
    }

    @Override
    protected Stream<? extends CypherResultRow> executeConnectedGetElements(VertexiumCypherQueryContext ctx, CypherResultRow row) {
        if (row.get(getResultName()) != null) {
            // TODO apply additional filters?
            return Stream.of(row);
        }

        List<? extends Element> connectedElements = getConnectedSteps().stream()
            .map(MatchPartExecutionStep::getResultName)
            .map(row::get)
            .map(e -> (Element) e)
            .collect(Collectors.toList());

        if (connectedElements.size() != 2) {
            throw new VertexiumCypherException("Expected 2 connected elements found " + connectedElements.size());
        }
        if (connectedElements.get(0) != null && !(connectedElements.get(0) instanceof Vertex)) {
            throw new VertexiumCypherException("Expected Vertex found " + connectedElements.get(0).getClass().getName());
        }
        if (connectedElements.get(1) != null && !(connectedElements.get(1) instanceof Vertex)) {
            throw new VertexiumCypherException("Expected Vertex found " + connectedElements.get(1).getClass().getName());
        }
        Vertex left = (Vertex) connectedElements.get(0);
        Vertex right = (Vertex) connectedElements.get(1);

        Set<EdgeData> edgeDatas = new HashSet<>();

        if (left != null) {
            Direction direction = toVertexiumQueryDirection(this.direction);
            Set<EdgeData> leftEdgeData = left.getEdgeIds(direction, ctx.getUser())
                .map(edgeId -> new EdgeData(left, edgeId))
                .collect(Collectors.toSet());
            edgeDatas.addAll(leftEdgeData);
        }

        // if this is a cyclic edge (left == right) don't add the edge id twice
        if (right != null && left != right) {
            Direction direction = toVertexiumQueryDirection(this.direction).reverse();
            Set<EdgeData> rightEdgeData = right.getEdgeIds(direction, ctx.getUser())
                .map(edgeId -> new EdgeData(right, edgeId))
                .collect(Collectors.toSet());
            edgeDatas.addAll(rightEdgeData);
        }

        edgeDatas = populateAndFilterEdgeData(ctx, row, edgeDatas);

        Stream<Object> result;
        if (range != null) {
            result = edgeDatas.stream()
                .map(edgeData -> new RelationshipRangePathResult(edgeData.source, edgeData.edge))
                .flatMap(path -> expandPath(ctx, row, path, range))
                .map(path -> new RelationshipRangePathResult(path.getElements().skip(1).collect(Collectors.toList())))
                .filter(p -> p.getTailElement() instanceof Edge)
                .distinct()
                .map(p -> p);
            if (range.getFrom() != null && range.getFrom() == 0) {
                result = Stream.concat(Stream.of(new RelationshipRangePathResult()), result);
            }
        } else {
            result = edgeDatas.stream().map(ed -> ed.edge);
        }

        Function<Object, CypherResultRow> transformResults = edge -> row.clone().set(getResultName(), edge);

        if (isOptional()) {
            return mapOptional(result, transformResults);
        } else {
            return result.map(transformResults);
        }
    }

    private Stream<PathResultBase> expandPath(
        VertexiumCypherQueryContext ctx,
        CypherResultRow row,
        RelationshipRangePathResult path,
        CypherRangeLiteral range
    ) {
        Stream<PathResultBase> results;
        if (range.getTo() != null && range.getTo() == 0) {
            return Stream.empty();
        }
        if (range.isInRange(path.getLength())) {
            results = Stream.of(path);
        } else {
            results = Stream.empty();
        }

        Element lastElement = path.getTailElement();
        if (lastElement instanceof Edge) {
            Edge edge = (Edge) lastElement;
            String vertexId = edge.getOtherVertexId(path.getLastVertex().getId());
            if (path.containsVertexId(vertexId)) {
                return results;
            }
            Vertex vertex = ctx.getGraph().getVertex(vertexId, ctx.getUser());
            return Stream.concat(results, expandPath(ctx, row, new RelationshipRangePathResult(path, vertex), range));
        } else if (lastElement instanceof Vertex) {
            Vertex vertex = (Vertex) lastElement;
            Set<EdgeData> edgeDatas = vertex.getEdgeIds(toVertexiumQueryDirection(this.direction), ctx.getUser())
                .map(edgeId -> new EdgeData(vertex, edgeId))
                .collect(Collectors.toSet());
            edgeDatas = populateAndFilterEdgeData(ctx, row, edgeDatas);
            Stream<PathResultBase> additionalPaths = edgeDatas.stream()
                .map(edgeData -> new RelationshipRangePathResult(path, edgeData.edge))
                .flatMap(p -> expandPath(ctx, row, p, range));
            return Stream.concat(results, additionalPaths);
        } else {
            throw new VertexiumCypherNotImplemented("Unhandled element type: " + lastElement.getClass().getName());
        }
    }

    private Set<EdgeData> populateAndFilterEdgeData(VertexiumCypherQueryContext ctx, CypherResultRow row, Set<EdgeData> edgeDatas) {
        Iterable<String> edgeIds = edgeDatas.stream().map(ed -> ed.edgeId).collect(Collectors.toList());
        Map<String, Edge> edgesById = ctx.getGraph().getEdges(edgeIds, ctx.getUser())
            .collect(Collectors.toMap(Element::getId, e -> e));
        return edgeDatas.stream()
            .peek(edgeData -> edgeData.edge = edgesById.get(edgeData.edgeId))
            .filter(edgeData -> edgeData.edge != null)
            .filter(edgeData -> doesEdgeMatch(ctx, row, edgeData.edge))
            .collect(Collectors.toSet());
    }

    private boolean doesEdgeMatch(VertexiumCypherQueryContext ctx, CypherResultRow row, Edge edge) {
        if (!doLabelNamesMatch(ctx, edge)) {
            return false;
        }
        return doPropertiesMatch(ctx, row, edge);
    }

    private boolean doLabelNamesMatch(VertexiumCypherQueryContext ctx, Edge edge) {
        if (relTypesNames.size() > 0) {
            Stream<String> labelNames = relTypesNames.stream()
                .map(ctx::normalizeLabelName);
            if (labelNames.noneMatch(ln -> edge.getLabel().equals(ln))) {
                return false;
            }
        }
        return true;
    }

    private Direction toVertexiumQueryDirection(CypherDirection direction) {
        if (direction == CypherDirection.BOTH || direction == CypherDirection.UNSPECIFIED) {
            return Direction.BOTH;
        }
        if (direction == CypherDirection.OUT) {
            return Direction.OUT;
        }
        if (direction == CypherDirection.IN) {
            return Direction.IN;
        }
        throw new VertexiumCypherException("Unhandled direction: " + direction);
    }

    public String getOtherVertexId(CypherResultRow row, MatchNodePartExecutionStep nodePartExecutionStep) {
        MatchNodePartExecutionStep otherExecutionStep = getOtherExecutionStep(nodePartExecutionStep);
        Vertex otherVertex = (Vertex) row.get(otherExecutionStep.getResultName());
        if (otherVertex != null) {
            Object value = row.get(getResultName());
            if (value == null) {
                if (row.has(getResultName())) {
                    return null;
                }
                throw new VertexiumCypherNotImplemented("could not find edge or path");
            }
            if (value instanceof Edge) {
                Edge edge = (Edge) value;
                return edge.getOtherVertexId(otherVertex.getId());
            } else if (value instanceof PathResultBase) {
                PathResultBase pathResult = (PathResultBase) value;
                return pathResult.getOtherVertexId(otherVertex.getId());
            } else {
                throw new VertexiumCypherNotImplemented("Unhandled type: " + value + " (class: " + value.getClass() + ")");
            }
        }
        return null;
    }

    private MatchNodePartExecutionStep getOtherExecutionStep(MatchNodePartExecutionStep nodePartExecutionStep) {
        if (getConnectedSteps().size() != 2) {
            throw new VertexiumCypherException("Expected 2 connected elements found " + getConnectedSteps().size());
        }
        if (getConnectedSteps().get(0) == nodePartExecutionStep) {
            return getConnectedSteps().get(1);
        }
        if (getConnectedSteps().get(1) == nodePartExecutionStep) {
            return getConnectedSteps().get(0);
        }
        throw new VertexiumCypherException("Could not find execution step on either end: " + nodePartExecutionStep);
    }

    private static class EdgeData {
        public final Vertex source;
        public final String edgeId;
        public Edge edge;

        public EdgeData(Vertex source, String edgeId) {
            this.source = source;
            this.edgeId = edgeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EdgeData edgeData = (EdgeData) o;
            return edgeId.equals(edgeData.edgeId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(edgeId);
        }
    }
}
