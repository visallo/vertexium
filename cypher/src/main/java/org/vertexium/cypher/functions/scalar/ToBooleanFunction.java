package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.TypeConversionFunction;

public class ToBooleanFunction extends TypeConversionFunction {
    @Override
    protected Object convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return value;
        }
        if (value instanceof String) {
            String str = ((String) value).trim();
            if (str.equalsIgnoreCase("true")) {
                return true;
            }
            if (str.equalsIgnoreCase("false")) {
                return false;
            }
            return null;
        }
        throw new VertexiumCypherTypeErrorException("InvalidArgumentValue: " + value);
    }
}
