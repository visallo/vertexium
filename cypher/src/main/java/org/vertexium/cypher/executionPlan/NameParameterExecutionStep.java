package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public class NameParameterExecutionStep extends DefaultExecutionStep implements ExecutionStepWithResultName {
    private final String resultName;
    private final String parameterName;

    public NameParameterExecutionStep(String resultName, String parameterName) {
        this.resultName = resultName;
        this.parameterName = parameterName;
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        return source.peek(row -> row.pushScope(getResultName(), ctx.getParameter(parameterName)));
    }

    @Override
    public String toString() {
        return String.format("%s {resultName='%s', parameterName='%s'}", super.toString(), resultName, parameterName);
    }
}
