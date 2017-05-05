package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.TypeConversionFunction;

public class ToFloatFunction extends TypeConversionFunction {
    @Override
    protected Object convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            String str = ((String) value).trim();
            try {
                return new Double(str);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        throw new VertexiumCypherTypeErrorException("InvalidArgumentValue: " + value);
    }
}
