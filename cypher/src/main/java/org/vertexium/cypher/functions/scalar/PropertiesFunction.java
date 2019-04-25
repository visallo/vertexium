package org.vertexium.cypher.functions.scalar;

import org.vertexium.Element;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherSyntaxErrorException;
import org.vertexium.cypher.executionPlan.ExecutionStepWithResultName;
import org.vertexium.cypher.executionPlan.GetVariableExecutionStep;
import org.vertexium.cypher.executionPlan.LiteralExecutionStep;
import org.vertexium.cypher.executionPlan.MapLiteralExecutionStep;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.util.Map;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class PropertiesFunction extends SimpleCypherFunction {
    @Override
    public ExecutionStepWithResultName create(String resultName, boolean distinct, ExecutionStepWithResultName[] argumentsExecutionStep) {
        ExecutionStepWithResultName arg0 = argumentsExecutionStep[0];
        if (arg0 == null
            || arg0 instanceof GetVariableExecutionStep
            || arg0 instanceof MapLiteralExecutionStep
            || (arg0 instanceof LiteralExecutionStep && ((LiteralExecutionStep) arg0).getValue() == null)) {
            return super.create(resultName, distinct, argumentsExecutionStep);
        }

        throw new VertexiumCypherSyntaxErrorException("InvalidArgumentType: properties(): expected variable or map, found " + arg0.getClass().getName());
    }

    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof Element) {
            return ctx.getElementPropertiesAsMap((Element) arg0);
        }

        if (arg0 instanceof Map) {
            return getPropertiesFromMap(ctx, (Map) arg0);
        }

        throw new VertexiumException("not implemented");
    }

    private Map<String, Object> getPropertiesFromMap(VertexiumCypherQueryContext ctx, Map map) {
        return map;
    }
}
