package org.vertexium.cypher.functions.string;

public class TrimFunction extends CypherUnaryStringFunction {
    @Override
    protected Object invokeOnString(String str) {
        return str.trim();
    }
}
