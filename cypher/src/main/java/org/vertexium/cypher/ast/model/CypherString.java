package org.vertexium.cypher.ast.model;

import org.vertexium.cypher.exceptions.VertexiumCypherSyntaxErrorException;
import org.vertexium.cypher.utils.StringUtils;

public class CypherString extends CypherLiteral<String> {
    public CypherString(String value) {
        super(unescape(value));
    }

    private static String unescape(String value) {
        try {
            return StringUtils.unescape(value);
        } catch (IllegalArgumentException ex) {
            String type = "Unknown";
            if (ex.getMessage().equals("string too short for \\u escape")) {
                type = "InvalidUnicodeLiteral";
            }
            throw new VertexiumCypherSyntaxErrorException(type + ": could not parse string \"" + value + "\"", ex);
        }
    }

    @Override
    public String toString() {
        return String.format("'%s'", getValue());
    }
}
