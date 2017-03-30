package org.vertexium.cypher.functions.string;

import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.TypeConversionFunction;

public class ToStringFunction extends TypeConversionFunction {
    @Override
    protected Object convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return value;
        }
        if (value instanceof Number
                || value instanceof Boolean) {
            return value.toString();
        }
        throw new VertexiumCypherTypeErrorException("InvalidArgumentValue: " + value + " (" + value.getClass().getName() + ")");
    }
}
