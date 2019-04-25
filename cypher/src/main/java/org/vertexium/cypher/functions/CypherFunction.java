package org.vertexium.cypher.functions;

import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;

public interface CypherFunction {
    ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep);
}
