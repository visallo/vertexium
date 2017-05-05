package org.vertexium.cypher.functions.string;

public class ToUpperFunction extends CypherUnaryStringFunction {
    @Override
    protected Object invokeOnString(String str) {
        return str.toUpperCase();
    }
}
