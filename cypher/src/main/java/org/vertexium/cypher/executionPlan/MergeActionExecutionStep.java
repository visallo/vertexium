package org.vertexium.cypher.executionPlan;

public class MergeActionExecutionStep extends ExecutionStepWithChildren {
    private final Type type;

    public MergeActionExecutionStep(Type type, ExecutionStep step) {
        super(step);
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        CREATE,
        MATCH
    }
}
