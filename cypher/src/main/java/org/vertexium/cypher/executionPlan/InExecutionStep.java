package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.utils.ObjectUtils;

public class InExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final String valueResultName;
    private final String arrayResultName;

    public InExecutionStep(String resultName, ExecutionStepWithResultName valueExpression, ExecutionStepWithResultName arrayExpression) {
        super(valueExpression, arrayExpression);
        this.resultName = resultName;
        this.valueResultName = valueExpression.getResultName();
        this.arrayResultName = arrayExpression.getResultName();
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object value = row.get(valueResultName);
            Object arrObject = row.get(arrayResultName);
            row.popScope(2);
            Object result;
            if (arrObject == null) {
                result = null;
            } else {
                Object[] arr = (Object[]) arrObject;
                if (value == null && arr.length == 0) {
                    result = false;
                } else if (value == null) {
                    result = null;
                } else {
                    boolean found = false;
                    boolean containsNull = false;
                    for (Object o : arr) {
                        if (o == null) {
                            containsNull = true;
                        } else if (ObjectUtils.equals(value, o)) {
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        result = true;
                    } else if (containsNull) {
                        result = null;
                    } else {
                        result = found;
                    }
                }
            }
            row.pushScope(getResultName(), result);
        });
    }

    @Override
    public String toString() {
        return String.format(
            "%s {resultName='%s', valueResultName='%s', arrayResultName='%s'}",
            super.toString(),
            resultName,
            valueResultName,
            arrayResultName
        );
    }
}
