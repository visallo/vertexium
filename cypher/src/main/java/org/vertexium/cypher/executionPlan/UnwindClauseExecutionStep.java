package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class UnwindClauseExecutionStep extends ExecutionStepWithChildren {
    private final String name;
    private final String expressionResultName;

    public UnwindClauseExecutionStep(String name, ExecutionStepWithResultName expression) {
        super(expression);
        this.name = name;
        this.expressionResultName = expression.getResultName();
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        if (source == null) {
            source = new SingleRowVertexiumCypherResult();
        }
        source = super.execute(ctx, source);
        return source.flatMapCypherResult(this::executeOnRow);
    }

    private Stream<CypherResultRow> executeOnRow(CypherResultRow row) {
        Object expressionResult = row.get(expressionResultName);
        row.popScope();

        if (expressionResult instanceof Iterable) {
            return executeOnStream(row, stream((Iterable<Object>) expressionResult));
        }

        if (expressionResult instanceof Stream) {
            return executeOnStream(row, (Stream<Object>) expressionResult);
        }

        if (expressionResult.getClass().isArray()) {
            Object[] values = (Object[]) expressionResult;
            return executeOnStream(row, Arrays.stream(values));
        }

        throw new VertexiumCypherNotImplemented("Unhandled type: " + expressionResult.getClass().getName());
    }

    private Stream<CypherResultRow> executeOnStream(CypherResultRow row, Stream<Object> expressionResult) {
        return expressionResult
            .map(item -> {
                CypherResultRow newRow = row.clone();
                newRow.set(name, item);
                return newRow;
            });
    }
}
