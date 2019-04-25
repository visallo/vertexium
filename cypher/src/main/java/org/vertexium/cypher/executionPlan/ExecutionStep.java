package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;

import java.util.stream.Stream;

public interface ExecutionStep {
    VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source);

    String toStringFull();

    Stream<ExecutionStep> getChildSteps();
}

