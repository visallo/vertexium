package org.vertexium.cypher.executionPlan;

import org.vertexium.Element;
import org.vertexium.Vertex;
import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.RelationshipRangePathResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherException;
import org.vertexium.search.Query;
import org.vertexium.search.QueryResults;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MatchNodePartExecutionStep extends MatchPartExecutionStep<MatchRelationshipPartExecutionStep> {
    private final Set<String> labelNames;

    public MatchNodePartExecutionStep(
        String originalName,
        String resultName,
        boolean optional,
        Set<String> labelNames,
        List<ExecutionStepWithResultName> properties
    ) {
        super(originalName, resultName, optional, properties);
        this.labelNames = labelNames;
    }

    @Override
    protected QueryResults<? extends Element> getElements(VertexiumCypherQueryContext ctx, Query q) {
        QueryResults<? extends Element> elements;
        for (String labelName : labelNames) {
            q = q.has(ctx.getLabelPropertyName(), ctx.normalizeLabelName(labelName));
        }
        elements = q.vertices(ctx.getFetchHints()); // TODO calculate best fetch hints
        return elements;
    }

    @Override
    protected Stream<? extends CypherResultRow> executeConnectedGetElements(VertexiumCypherQueryContext ctx, CypherResultRow row) {
        if (row.get(getResultName()) != null) {
            // TODO apply additional filters?
            return Stream.of(row);
        }

        if (getConnectedSteps().size() == 0) {
            throw new VertexiumCypherException("Should be using getElements not connected elements");
        }

        if (isOptional() && isAllConnectedStepsCompletedAndNull(row)) {
            row.set(getResultName(), null);
            return Stream.of(row);
        }

        Set<String> vertexIds = getConnectedSteps().stream()
            .map(step -> step.getOtherVertexId(row, this))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        if (vertexIds.size() == 0) {
            if (isConnectedToZeroLengthEdge(row)) {
                return executeInitialQuery(ctx, row);
            }
            throw new VertexiumCypherException("Failed to find other vertex ids");
        }
        if (vertexIds.size() != 1) {
            throw new VertexiumCypherException("expecting only a single vertex but found: " + vertexIds.size());
        }
        String vertexId = vertexIds.iterator().next();
        Vertex vertex = ctx.getGraph().getVertex(vertexId, ctx.getUser()); // TODO fetch hints
        if (vertex == null) {
            throw new VertexiumCypherException("could not find vertex " + vertexId);
        }

        if (labelNames.size() > 0) {
            Set<String> vertexLabels = ctx.getVertexLabels(vertex);
            for (String labelName : labelNames) {
                if (!vertexLabels.contains(ctx.normalizeLabelName(labelName))) {
                    return Stream.empty();
                }
            }
        }

        row.set(getResultName(), vertex);
        return Stream.of(row);
    }

    private boolean isConnectedToZeroLengthEdge(CypherResultRow row) {
        return getConnectedSteps().stream()
            .anyMatch(step -> {
                Object stepValue = row.get(step.getResultName());
                if (stepValue instanceof RelationshipRangePathResult) {
                    RelationshipRangePathResult stepPathResult = (RelationshipRangePathResult) stepValue;
                    if (stepPathResult.getLength() == 0) {
                        return true;
                    }
                }
                return false;
            });
    }

    private boolean isAllConnectedStepsCompletedAndNull(CypherResultRow row) {
        return getConnectedSteps().stream()
            .allMatch(step -> row.get(step.getResultName()) == null);
    }

    @Override
    public String toString() {
        return String.format("%s {labelNames=%s}", super.toString(), labelNames);
    }
}
