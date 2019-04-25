package org.vertexium.cypher.functions.predicate;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.functions.SimpleCypherFunction;

public class AllFunction extends SimpleCypherFunction {
    @Override
    protected Object executeFunction(VertexiumCypherQueryContext ctx, Object[] arguments) {
        throw new VertexiumCypherNotImplemented("" + this.getClass().getName());
    }
}
