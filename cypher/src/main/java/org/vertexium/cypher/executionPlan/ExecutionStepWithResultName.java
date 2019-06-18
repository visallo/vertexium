package org.vertexium.cypher.executionPlan;

public interface ExecutionStepWithResultName extends ExecutionStep {
    String getResultName();

    static String[] getResultNames(ExecutionStepWithResultName[] argumentsExecutionStep) {
        String[] argumentResultNames = new String[argumentsExecutionStep.length];
        for (int i = 0; i < argumentsExecutionStep.length; i++) {
            argumentResultNames[i] = argumentsExecutionStep[i].getResultName();
        }
        return argumentResultNames;
    }
}
