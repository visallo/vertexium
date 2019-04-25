package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.model.CypherLiteral;

public class LiteralExecutionStep extends DefaultExecutionStep implements ExecutionStepWithResultName {
    private final String resultName;
    private final Object value;

    public LiteralExecutionStep(String resultName, Object value) {
        this.resultName = resultName;
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        if (source == null) {
            source = new SingleRowVertexiumCypherResult();
        }
        return source.peek(row -> row.pushScope(resultName, CypherLiteral.toJava(value)));
    }

    @Override
    public String toString() {
        return String.format("%s {resultName='%s', value=%s}", super.toString(), resultName, value);
    }

    @Override
    public String getResultName() {
        return resultName;
    }
}
