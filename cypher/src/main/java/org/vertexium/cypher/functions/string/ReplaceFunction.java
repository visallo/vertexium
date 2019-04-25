package org.vertexium.cypher.functions.string;

import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

public class ReplaceFunction extends CypherUnaryStringFunction {
    @Override
    protected Object invokeOnString(String str) {
        throw new VertexiumCypherNotImplemented("" + this.getClass().getName());
    }
}
