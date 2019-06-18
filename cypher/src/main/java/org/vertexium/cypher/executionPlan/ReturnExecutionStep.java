package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.utils.ObjectUtils;
import org.vertexium.cypher.utils.PredicateWithIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.distinctBy;

public class ReturnExecutionStep extends ExecutionStepWithChildren {
    private final boolean distinct;
    private final String skipExpressionResultName;
    private final String limitExpressionResultName;
    private final List<SortItemExecutionStep> sortItems;

    public ReturnExecutionStep(
        boolean distinct,
        List<ExecutionStep> returnItems,
        ExecutionStepWithResultName skipExpression,
        ExecutionStepWithResultName limitExpression,
        List<SortItemExecutionStep> sortItems
    ) {
        super(toChildren(returnItems, skipExpression, limitExpression, sortItems));
        this.distinct = distinct;
        this.skipExpressionResultName = skipExpression == null ? null : skipExpression.getResultName();
        this.limitExpressionResultName = limitExpression == null ? null : limitExpression.getResultName();
        this.sortItems = sortItems == null ? null : new ArrayList<>(sortItems);
    }

    private static ExecutionStep[] toChildren(
        List<ExecutionStep> returnItems,
        ExecutionStepWithResultName skipExpression,
        ExecutionStepWithResultName limitExpression,
        List<SortItemExecutionStep> sortItems
    ) {
        return Stream.concat(
            Stream.concat(
                returnItems.stream(),
                Stream.of(skipExpression, limitExpression)
            ),
            sortItems == null ? Stream.of() : sortItems.stream()
        )
            .filter(Objects::nonNull)
            .toArray(ExecutionStep[]::new);
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        if (source == null) {
            source = new SingleRowVertexiumCypherResult();
        }

        source.getColumnNames().clear();
        VertexiumCypherResult result = super.execute(ctx, source);
        if (distinct) {
            result = result.filter(distinctBy(row -> {
                HashMap<String, Object> columnValues = new HashMap<>();
                for (String columnName : row.getColumnNames()) {
                    columnValues.put(columnName, row.get(columnName));
                }
                return columnValues;
            }));
        }
        result = applySort(result);
        result = applySkip(result);
        result = applyLimit(result);
        return result;
    }

    private VertexiumCypherResult applyLimit(VertexiumCypherResult result) {
        if (limitExpressionResultName != null) {
            result = result.filter(new PredicateWithIndex<CypherResultRow>() {
                @Override
                protected boolean test(CypherResultRow row, long index) {
                    Number value = (Number) row.get(limitExpressionResultName);
                    long limit = value.longValue();
                    return index < limit;
                }
            });
        }
        return result;
    }

    private VertexiumCypherResult applySkip(VertexiumCypherResult result) {
        if (skipExpressionResultName != null) {
            result = result.filter(new PredicateWithIndex<CypherResultRow>() {
                @Override
                protected boolean test(CypherResultRow row, long index) {
                    Number value = (Number) row.get(skipExpressionResultName);
                    long skip = value.longValue();
                    return index >= skip;
                }
            });
        }
        return result;
    }

    private VertexiumCypherResult applySort(VertexiumCypherResult result) {
        if (sortItems != null) {
            result = result.sorted((row1, row2) -> {
                for (SortItemExecutionStep sortItem : sortItems) {
                    Object value1 = row1.get(sortItem.getResultName());
                    Object value2 = row2.get(sortItem.getResultName());
                    int r = ObjectUtils.compare(value1, value2);
                    if (r != 0) {
                        switch (sortItem.getDirection()) {
                            case ASCENDING:
                                return r;
                            case DESCENDING:
                                return -r;
                            default:
                                throw new VertexiumCypherNotImplemented("Invalid direction: " + sortItem.getDirection());
                        }
                    }
                }
                return 0;
            });
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s {distinct=%s}", super.toString(), distinct);
    }
}

