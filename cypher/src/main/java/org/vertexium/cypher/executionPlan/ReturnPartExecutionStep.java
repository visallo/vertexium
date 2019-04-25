package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReturnPartExecutionStep extends ExecutionStepWithChildren {
    private final String columnName;
    private final String expressionResultName;

    public ReturnPartExecutionStep(
        String alias,
        String originalText,
        ExecutionStepWithResultName expression
    ) {
        super(Stream.of(expression).filter(Objects::nonNull).toArray(ExecutionStep[]::new));
        this.columnName = alias != null ? alias : originalText;
        this.expressionResultName = expression == null ? null : expression.getResultName();
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);

        if (expressionResultName != null) {
            source = source.peek(row -> {
                row.set(columnName, row.get(expressionResultName));
                row.popScope();
            });
        }
        LinkedHashSet<String> columnNames = source.getColumnNames();
        if (columnName.equals("*")) {
            List<String> allColumnNames = getAllColumnNames(ctx.getCurrentlyExecutingPlan()).stream()
                .sorted()
                .collect(Collectors.toList());
            columnNames.addAll(allColumnNames);
        } else {
            columnNames.add(columnName);
        }
        return new VertexiumCypherResult(source, columnNames);
    }

    private Set<String> getAllColumnNames(ExecutionPlan plan) {
        return getAllColumnNames(plan.getRoot());
    }

    private Set<String> getAllColumnNames(ExecutionStep step) {
        Set<String> results = new HashSet<>();
        if (step instanceof ReturnPartExecutionStep) {
            String columnName = ((ReturnPartExecutionStep) step).getColumnName();
            if (!columnName.equals("*")) {
                results.add(columnName);
            }
        } else if (step instanceof MatchPartExecutionStep) {
            MatchPartExecutionStep matchPartStep = (MatchPartExecutionStep) step;
            String name = matchPartStep.getOriginalName();
            if (name != null) {
                results.add(name);
            }
        } else {
            step.getChildSteps().forEach(childStep -> {
                if (childStep instanceof WithClauseExecutionStep) {
                    results.clear();
                }
                results.addAll(getAllColumnNames(childStep));
            });
        }
        return results;
    }

    @Override
    public String toString() {
        return String.format("%s {columnName=%s, expressionResultName=%s}", super.toString(), columnName, expressionResultName);
    }

    public String getColumnName() {
        return columnName;
    }
}
