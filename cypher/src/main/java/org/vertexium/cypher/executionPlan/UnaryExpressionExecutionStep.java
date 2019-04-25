package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.model.CypherUnaryExpression;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

public class UnaryExpressionExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final CypherUnaryExpression.Op op;
    private final String expressionResultName;

    public UnaryExpressionExecutionStep(
        String resultName,
        CypherUnaryExpression.Op op,
        ExecutionStepWithResultName expression
    ) {
        super(expression);
        this.resultName = resultName;
        this.expressionResultName = expression.getResultName();
        this.op = op;
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object value = row.get(expressionResultName);
            row.popScope();
            Object result;
            switch (op) {
                case NOT:
                    result = executeNot(value);
                    break;
                default:
                    throw new VertexiumCypherNotImplemented("unhandled op " + op);
            }
            row.pushScope(getResultName(), result);
        });
    }

    private Object executeNot(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return !((boolean) value);
        }
        throw new VertexiumCypherNotImplemented("Cannot NOT a non-boolean or null value");
    }

    @Override
    public String toString() {
        return String.format(
            "%s {resultName='%s', op=%s, expressionResultName='%s'}",
            super.toString(),
            resultName,
            op,
            expressionResultName
        );
    }
}
