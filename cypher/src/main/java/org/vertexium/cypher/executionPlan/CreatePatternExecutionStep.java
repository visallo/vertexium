package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

import java.util.List;

public class CreatePatternExecutionStep extends ExecutionStepWithChildren {
    private final String name;

    public CreatePatternExecutionStep(
        String name,
        List<CreateNodePatternExecutionStep> createNodeExecutionSteps,
        List<CreateRelationshipPatternExecutionStep> createRelationshipExecutionSteps
    ) {
        super(toChildren(createNodeExecutionSteps, createRelationshipExecutionSteps));
        this.name = name;
    }

    private static ExecutionStep[] toChildren(
        List<CreateNodePatternExecutionStep> createNodeExecutionSteps,
        List<CreateRelationshipPatternExecutionStep> createRelationshipExecutionSteps
    ) {
        ExecutionStep[] results = new ExecutionStep[createNodeExecutionSteps.size() + createRelationshipExecutionSteps.size()];
        int i = 0;
        for (CreateNodePatternExecutionStep createNodeExecutionStep : createNodeExecutionSteps) {
            results[i++] = createNodeExecutionStep;
        }
        for (CreateRelationshipPatternExecutionStep createRelationshipExecutionStep : createRelationshipExecutionSteps) {
            results[i++] = createRelationshipExecutionStep;
        }
        return results;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        if (source == null) {
            source = new SingleRowVertexiumCypherResult();
        }

        source = super.execute(ctx, source);

        if (name != null) {
            throw new VertexiumCypherNotImplemented("name");
        }

        return source;
    }

    @Override
    public String toString() {
        return String.format("%s {name=%s}", super.toString(), name);
    }
}
