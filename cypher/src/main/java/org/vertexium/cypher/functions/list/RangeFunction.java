package org.vertexium.cypher.functions.list;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherArgumentErrorException;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;
import org.vertexium.cypher.executionPlan.FunctionInvocationExecutionStep;
import org.vertexium.cypher.functions.CypherFunction;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class RangeFunction implements CypherFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        if (distinct) {
            throw new VertexiumCypherNotImplemented("distinct");
        }
        return new FunctionInvocationExecutionStep(getClass().getSimpleName(), resultName, argumentsExecutionStep) {
            @Override
            protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
                if (arguments.length > 3) {
                    throw new VertexiumException("range function takes 2 or 3 arguments");
                }
                int arg0 = argumentToInt(arguments[0]);
                int arg1 = argumentToInt(arguments[1]);
                Stream<Integer> result = IntStream.rangeClosed(arg0, arg1).boxed();
                if (arguments.length == 3) {
                    int step = argumentToInt(arguments[2]);
                    if (step == 0) {
                        throw new VertexiumCypherArgumentErrorException("NumberOutOfRange: step must be greater than 0");
                    }
                    result = result.filter(i -> i % step == 0);
                }
                return result;
            }
        };
    }

    private int argumentToInt(Object argument) {
        if (argument instanceof Long) {
            return (int) ((long) argument);
        }
        if (argument instanceof Integer) {
            return (int) argument;
        }
        throw new VertexiumException("Could not convert argument \"" + argument + "\" to int");
    }
}
