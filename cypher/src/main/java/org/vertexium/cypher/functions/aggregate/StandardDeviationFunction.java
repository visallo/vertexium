package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;

public class StandardDeviationFunction implements AggregationFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        throw new VertexiumCypherNotImplemented("" + this.getClass().getName());
    }
}
