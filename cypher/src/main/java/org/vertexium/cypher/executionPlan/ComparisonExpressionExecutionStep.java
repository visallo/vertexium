package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.utils.ObjectUtils;

public class ComparisonExpressionExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final String op;
    private final String leftResultName;
    private final String rightResultName;

    public ComparisonExpressionExecutionStep(
        String resultName,
        String op,
        ExecutionStepWithResultName left,
        ExecutionStepWithResultName right
    ) {
        super(left, right);
        this.op = op;
        this.leftResultName = left.getResultName();
        this.rightResultName = right.getResultName();
        this.resultName = resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object leftValue = row.get(leftResultName);
            Object rightValue = row.get(rightResultName);
            row.popScope(2);
            if (leftValue == null && rightValue == null) {
                row.pushScope(resultName, null);
            } else {
                int compareResults = ObjectUtils.compare(leftValue, rightValue);
                boolean result;
                switch (op) {
                    case "=":
                        result = compareResults == 0;
                        break;
                    case ">":
                        result = compareResults > 0;
                        break;
                    case ">=":
                        result = compareResults >= 0;
                        break;
                    case "<":
                        result = compareResults < 0;
                        break;
                    case "<=":
                        result = compareResults <= 0;
                        break;
                    case "<>":
                        result = compareResults != 0;
                        break;
                    default:
                        throw new VertexiumCypherNotImplemented("op not implemented: " + op);
                }
                row.pushScope(resultName, result);
            }
        });
    }

    @Override
    public String toString() {
        return String.format(
            "%s {resultName='%s', op='%s', leftResultName='%s', rightResultName='%s'}",
            super.toString(),
            resultName,
            op,
            leftResultName,
            rightResultName
        );
    }

    @Override
    public String getResultName() {
        return resultName;
    }
}

