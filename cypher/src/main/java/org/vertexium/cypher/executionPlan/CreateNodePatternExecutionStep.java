package org.vertexium.cypher.executionPlan;

import org.vertexium.ElementType;
import org.vertexium.VertexBuilder;
import org.vertexium.Visibility;
import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.mutation.ElementMutation;

import java.util.List;

public class CreateNodePatternExecutionStep extends CreateElementPatternExecutionStep {
    private final List<String> labelNames;

    public CreateNodePatternExecutionStep(
        String name,
        List<String> labelNames,
        List<ExecutionStepWithResultName> properties,
        List<ExecutionStep> mergeActions
    ) {
        super(ElementType.VERTEX, name, properties, mergeActions);
        this.labelNames = labelNames;
    }

    @Override
    protected ElementMutation createElement(VertexiumCypherQueryContext ctx, CypherResultRow row) {
        String vertexId = ctx.calculateVertexId(this, row);
        Visibility visibility = ctx.calculateVertexVisibility(this, row);
        VertexBuilder m = ctx.getGraph().prepareVertex(vertexId, visibility);
        for (String labelName : labelNames) {
            ctx.setLabelProperty(m, ctx.normalizeLabelName(labelName));
        }

        return m;
    }

    @Override
    public String toString() {
        return String.format("%s {labelNames=%s}", super.toString(), String.join(", ", labelNames));
    }
}
