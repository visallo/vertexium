package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.CypherResultRow;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public abstract class FunctionInvocationExecutionStepBase extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String functionName;
    private final String resultName;
    private final String[] argumentResultNames;

    public FunctionInvocationExecutionStepBase(
        String functionName,
        String resultName,
        ExecutionStepWithResultName[] argumentsExecutionStep
    ) {
        super(argumentsExecutionStep);
        this.functionName = functionName;
        this.resultName = resultName;
        this.argumentResultNames = ExecutionStepWithResultName.getResultNames(argumentsExecutionStep);
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    protected String[] getArgumentResultNames() {
        return argumentResultNames;
    }

    protected Object[] getArguments(CypherResultRow row) {
        String[] argumentResultNames = getArgumentResultNames();
        Object[] arguments = new Object[argumentResultNames.length];
        for (int i = 0; i < argumentResultNames.length; i++) {
            arguments[i] = row.get(argumentResultNames[i]);
        }
        row.popScope(argumentResultNames.length);
        return arguments;
    }

    protected Stream<RowWithArguments> getRowsWithArguments(Stream<CypherResultRow> source, int... expectedCounts) {
        return source.map(row -> {
            Object[] arguments = getArguments(row);
            if (expectedCounts != null && expectedCounts.length > 0) {
                assertArgumentCount(arguments, expectedCounts);
            }
            return new RowWithArguments(row, arguments);
        });
    }

    @Override
    public String toString() {
        return String.format(
            "%s {resultName='%s', argumentResultNames=%s}",
            functionName,
            resultName,
            Arrays.toString(argumentResultNames)
        );
    }

    protected static class RowWithArguments {
        public final CypherResultRow row;
        public final Object[] arguments;

        public RowWithArguments(CypherResultRow row, Object[] arguments) {
            this.row = row;
            this.arguments = arguments;
        }
    }
}
