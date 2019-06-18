package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.*;
import org.vertexium.cypher.utils.ObjectUtils;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AggregationFunctionInvocationExecutionStep extends FunctionInvocationExecutionStepBase {
    private final boolean distinct;

    public AggregationFunctionInvocationExecutionStep(
        String functionName,
        String resultName,
        boolean distinct,
        ExecutionStepWithResultName[] argumentsExecutionStep
    ) {
        super(functionName, resultName, argumentsExecutionStep);
        this.distinct = distinct;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        Map<CypherResultRow, List<CypherResultRow>> rowGroups = source
            .collect(Collectors.groupingBy(row -> {
                Map<String, Object> values = new HashMap<>();
                for (String columnName : row.getColumnNames()) {
                    values.put(columnName, row.get(columnName));
                }
                return new DefaultCypherResultRow(row.getColumnNames(), values);
            }));

        if (rowGroups.size() == 0) {
            CypherResultRow fakeRow = new DefaultCypherResultRow(source.getColumnNames(), new HashMap<>());
            CypherResultRow newRow = executeAggregation(ctx, fakeRow, null);
            return new SingleRowVertexiumCypherResult(newRow);
        }

        AtomicReference<LinkedHashSet<String>> columnNames = new AtomicReference<>(source.getColumnNames());
        Stream<CypherResultRow> newRows = rowGroups.entrySet().stream()
            .map(entry -> {
                VertexiumCypherResult rowsResult = new VertexiumCypherResult(entry.getValue().stream(), source.getColumnNames());
                Stream<CypherResultRow> rows = super.execute(ctx, rowsResult);
                Stream<RowWithArguments> rowsWithArguments = getRowsWithArguments(rows, getExpectedArgumentCount());
                if (distinct) {
                    rowsWithArguments = rowsWithArguments.filter(ObjectUtils.distinctByDeep(row -> row.arguments[0]));
                }
                CypherResultRow group = entry.getValue().size() > 0 ? entry.getValue().get(0) : entry.getKey();
                CypherResultRow newRow = executeAggregation(ctx, group, rowsWithArguments);
                columnNames.set(newRow.getColumnNames());
                return newRow;
            });
        return new VertexiumCypherResult(newRows, columnNames.get());
    }

    protected int getExpectedArgumentCount() {
        return 1;
    }

    protected abstract CypherResultRow executeAggregation(VertexiumCypherQueryContext ctx, CypherResultRow group, Stream<RowWithArguments> rows);
}
