package org.vertexium.cypher.functions.math;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import java.util.Random;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class RandFunction extends SimpleCypherFunction {
    private final Random random = new Random();

    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 0);
        return random.nextDouble();
    }
}
