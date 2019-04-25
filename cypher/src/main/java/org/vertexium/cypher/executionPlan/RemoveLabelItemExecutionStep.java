package org.vertexium.cypher.executionPlan;

import org.vertexium.Vertex;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.model.CypherLabelName;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.mutation.ExistingElementMutation;

public class RemoveLabelItemExecutionStep extends ExecutionStepWithChildren {
    private final String leftResultName;
    private final Iterable<CypherLabelName> labelNames;

    public RemoveLabelItemExecutionStep(ExecutionStepWithResultName left, Iterable<CypherLabelName> labelNames) {
        super(left);
        this.leftResultName = left.getResultName();
        this.labelNames = labelNames;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object left = row.get(leftResultName);
            if (left instanceof Vertex) {
                executeRemoveLabels(ctx, (Vertex) left);
            } else {
                throw new VertexiumCypherNotImplemented("left is not an vertex: " + left.getClass().getName());
            }
        });
    }

    private void executeRemoveLabels(VertexiumCypherQueryContext ctx, Vertex vertex) {
        ExistingElementMutation<Vertex> m = vertex.prepareMutation();
        for (CypherLabelName labelName : labelNames) {
            ctx.removeLabel(m, labelName.getValue());
        }
        ctx.saveElement(m);
    }
}
