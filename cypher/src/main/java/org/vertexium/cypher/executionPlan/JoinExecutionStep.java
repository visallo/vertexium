package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.*;

import java.util.LinkedHashSet;
import java.util.stream.Stream;

public class JoinExecutionStep extends ExecutionStepWithChildren {
    private final boolean executeOnceOnEmptySource;

    public JoinExecutionStep(boolean executeOnceOnEmptySource, ExecutionStep... childSteps) {
        super(childSteps);
        this.executeOnceOnEmptySource = executeOnceOnEmptySource;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        if (source == null) {
            if (executeOnceOnEmptySource) {
                source = new SingleRowVertexiumCypherResult();
            } else {
                return new EmptyVertexiumCypherResult();
            }
        }

        LinkedHashSet<String> columnNames = source.getColumnNames();

        Stream<CypherResultRow> rows = source.flatMap(row -> super.execute(ctx, new SingleRowVertexiumCypherResult(row)));

        return new VertexiumCypherResult(rows, columnNames);
    }

    @Override
    public String toString() {
        return String.format("%s {executeOnceOnEmptySource=%s}", super.toString(), executeOnceOnEmptySource);
    }
}
