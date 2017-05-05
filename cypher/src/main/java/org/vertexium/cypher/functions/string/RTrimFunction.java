package org.vertexium.cypher.functions.string;

public class RTrimFunction extends CypherUnaryStringFunction {
    @Override
    protected Object invokeOnString(String str) {
        return str.replaceAll("\\s+$", "");
    }
}
