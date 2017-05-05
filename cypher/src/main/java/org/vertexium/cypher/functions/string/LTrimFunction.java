package org.vertexium.cypher.functions.string;

public class LTrimFunction extends CypherUnaryStringFunction {
    @Override
    protected Object invokeOnString(String str) {
        return str.replaceAll("^\\s+", "");
    }
}
