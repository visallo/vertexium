package org.vertexium.cypher.functions.list;

import org.vertexium.Edge;
import org.vertexium.cypher.PathResultBase;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.SimpleCypherFunction;

import static org.vertexium.cypher.functions.FunctionUtils.assertArgumentCount;

public class RelationshipsFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        assertArgumentCount(arguments, 1);
        Object arg0 = arguments[0];

        if (arg0 == null) {
            return null;
        }

        if (arg0 instanceof PathResultBase) {
            PathResultBase pathResult = (PathResultBase) arg0;
            return pathResult.getEdges().toArray(Edge[]::new);
        }

        throw new VertexiumCypherTypeErrorException(arg0, PathResultBase.class, null);
    }
}
