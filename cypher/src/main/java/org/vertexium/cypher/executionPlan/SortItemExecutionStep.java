package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.model.CypherSortItem;

import java.util.stream.Stream;

public class SortItemExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final CypherSortItem.Direction direction;
    private final String itemResultName;
    private final String expressionText;

    public SortItemExecutionStep(CypherSortItem.Direction direction, ExecutionStepWithResultName itemExpression, String expressionText) {
        super(itemExpression);
        this.direction = direction;
        this.itemResultName = itemExpression.getResultName();
        this.expressionText = expressionText;
    }

    public CypherSortItem.Direction getDirection() {
        return direction;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        return source.flatMapCypherResult(row -> {
            Object value = row.get(expressionText);
            if (value != null) {
                return Stream.of(row.pushScope(getResultName(), value));
            } else {
                return super.execute(ctx, new SingleRowVertexiumCypherResult(row));
            }
        });
    }

    @Override
    public String getResultName() {
        return itemResultName;
    }

    @Override
    public String toString() {
        return String.format(
            "%s {direction=%s, itemResultName='%s', expressionText='%s'}",
            super.toString(),
            direction,
            itemResultName,
            expressionText
        );
    }
}
