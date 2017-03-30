package org.vertexium.cypher.functions.scalar;

import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.TypeConversionFunction;

public class ToIntegerFunction extends TypeConversionFunction {
    @Override
    protected Object convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            String str = ((String) value).trim();
            try {
                return Integer.parseInt(str);
            } catch (NumberFormatException ex1) {
                try {
                    return new Double(str).intValue();
                } catch (NumberFormatException ex2) {
                    return null;
                }
            }
        }
        throw new VertexiumCypherTypeErrorException("InvalidArgumentValue: " + value);
    }
}
