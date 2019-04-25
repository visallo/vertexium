package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

public class ExecutionPlan {
    private final ExecutionStep root;

    public ExecutionPlan(ExecutionStep root) {
        this.root = root;
    }

    public String toStringFull() {
        return root.toStringFull();
    }

    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx) {
        ctx.setCurrentlyExecutingPlan(this);
        return root.execute(ctx, null);
    }

    public ExecutionStep getRoot() {
        return root;
    }
}
