package org.vertexium.cypher.functions.list;

import org.vertexium.Element;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;
import org.vertexium.cypher.executionPlan.FunctionInvocationExecutionStep;
import org.vertexium.cypher.functions.CypherFunction;

import java.util.Map;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class KeysFunction implements CypherFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        if (distinct) {
            throw new VertexiumCypherNotImplemented("distinct");
        }
        return new FunctionInvocationExecutionStep(getClass().getSimpleName(), resultName, argumentsExecutionStep) {
            @Override
            protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
                assertArgumentCount(arguments, 1);
                Object arg0 = arguments[0];

                if (arg0 instanceof Element) {
                    return ctx.getKeys((Element) arg0);
                }

                if (arg0 instanceof Map) {
                    return ((Map) arg0).keySet();
                }

                throw new VertexiumCypherTypeErrorException(arg0, Element.class, Map.class);
            }
        };
    }
}
