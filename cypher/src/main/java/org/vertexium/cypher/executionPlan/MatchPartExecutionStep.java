package org.vertexium.cypher.executionPlan;

import org.vertexium.Element;
import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.Query;
import org.vertexium.query.QueryResultsIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public abstract class MatchPartExecutionStep<TC extends MatchPartExecutionStep>
    extends ExecutionStepWithChildren
    implements ExecutionStepWithResultName {
    private final String resultName;
    private final boolean optional;
    private final List<String> propertyResultNames;
    private final List<TC> connectedSteps = new ArrayList<>();
    private final String originalName;

    public MatchPartExecutionStep(
        String originalName,
        String resultName,
        boolean optional,
        List<ExecutionStepWithResultName> properties
    ) {
        super(properties.toArray(new ExecutionStepWithResultName[0]));
        this.originalName = originalName;
        this.resultName = resultName;
        this.optional = optional;
        this.propertyResultNames = properties.stream().map(ExecutionStepWithResultName::getResultName).collect(Collectors.toList());
    }

    public String getOriginalName() {
        return originalName;
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    protected List<TC> getConnectedSteps() {
        return connectedSteps;
    }

    protected boolean isOptional() {
        return optional;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult originalSource) {
        VertexiumCypherResult source = originalSource == null ? new SingleRowVertexiumCypherResult() : originalSource;
        source = super.execute(ctx, source);

        if (originalSource == null || getConnectedSteps().size() == 0) {
            return new VertexiumCypherResult(
                source.flatMap(row -> executeInitialQuery(ctx, row)),
                source.getColumnNames()
            );
        }

        source = super.execute(ctx, source);
        return executeConnectedQuery(ctx, source);
    }

    protected Stream<CypherResultRow> executeInitialQuery(VertexiumCypherQueryContext ctx, CypherResultRow row) {
        GraphQuery q = ctx.getGraph().query(ctx.getAuthorizations());
        for (String propertyResultName : propertyResultNames) {
            String propertyName = ctx.normalizePropertyName(propertyResultName);
            Object value = row.get(propertyResultName);
            if (!ctx.getGraph().isPropertyDefined(propertyName)) {
                ctx.defineProperty(propertyName, value);
            }
            q.has(propertyName, value);
        }

        QueryResultsIterable<? extends Element> elements = getElements(ctx, q);
        if (elements.getTotalHits() == 0 && isOptional()) {
            CypherResultRow newRow = row.clone()
                .set(getResultName(), null);
            return new SingleRowVertexiumCypherResult(newRow);
        }

        return stream(elements)
            .map(element -> {
                CypherResultRow newRow = row.clone();
                newRow.set(getResultName(), element);
                return newRow;
            });
    }

    protected abstract QueryResultsIterable<? extends Element> getElements(VertexiumCypherQueryContext ctx, Query q);

    public VertexiumCypherResult executeConnectedQuery(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        return source.flatMapCypherResult(row -> executeConnectedGetElements(ctx, row));
    }

    protected abstract Stream<? extends CypherResultRow> executeConnectedGetElements(VertexiumCypherQueryContext ctx, CypherResultRow row);

    public void addConnectedStep(TC connectedStep) {
        connectedSteps.add(connectedStep);
    }

    protected boolean doPropertiesMatch(VertexiumCypherQueryContext ctx, CypherResultRow row, Element element) {
        if (propertyResultNames.size() == 0) {
            return true;
        }
        for (String propertyResultName : propertyResultNames) {
            String propertyName = ctx.normalizePropertyName(propertyResultName);
            Object value = row.get(propertyResultName);
            for (Object propertyValue : element.getPropertyValues(propertyName)) {
                if (propertyValue.equals(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format(
            "%s {propertyResultNames=[%s], resultName=%s, optional=%s, connectedSteps=[%s]}",
            super.toString(),
            String.join(", ", propertyResultNames),
            getResultName(),
            isOptional(),
            getConnectedSteps().stream()
                .map(MatchPartExecutionStep::getResultName)
                .collect(Collectors.joining(", "))
        );
    }
}
