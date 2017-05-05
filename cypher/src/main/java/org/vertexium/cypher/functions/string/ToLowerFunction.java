package org.vertexium.cypher.functions.string;

public class ToLowerFunction extends CypherUnaryStringFunction {
    @Override
    protected Object invokeOnString(String str) {
        return str.toLowerCase();
    }
}
