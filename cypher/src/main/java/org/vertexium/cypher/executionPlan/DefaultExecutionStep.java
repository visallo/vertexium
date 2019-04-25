package org.vertexium.cypher.executionPlan;

import java.util.stream.Stream;

public abstract class DefaultExecutionStep implements ExecutionStep {
    public String toStringFull() {
        return toString();
    }

    @Override
    public String toString() {
        String name = getClass().getSimpleName();
        if (name.length() == 0) {
            name = getClass().getName();
            int l = name.lastIndexOf('.');
            if (l >= 0) {
                name = name.substring(l + 1);
            }
        }
        return name;
    }

    @Override
    public Stream<ExecutionStep> getChildSteps() {
        return Stream.empty();
    }
}
