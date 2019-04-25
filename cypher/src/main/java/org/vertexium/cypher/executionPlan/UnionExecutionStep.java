package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

import java.util.LinkedHashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnionExecutionStep extends ExecutionStepWithChildren {
    private final boolean all;

    public UnionExecutionStep(boolean all, ExecutionStep left, ExecutionStep right) {
        super(left, right);
        this.all = all;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        LinkedHashSet<String> columnNames = new LinkedHashSet<>();
        Stream<CypherResultRow> rows = Stream.of();
        for (ExecutionStep step : getChildSteps().collect(Collectors.toList())) {
            VertexiumCypherResult result = step.execute(ctx, null);
            columnNames.addAll(result.getColumnNames());
            rows = Stream.concat(rows, result);
        }
        if (!all) {
            rows = rows.distinct();
        }
        return new VertexiumCypherResult(rows, columnNames);
    }

    @Override
    public String toString() {
        return String.format("%s {all=%s}", super.toString(), all);
    }
}
