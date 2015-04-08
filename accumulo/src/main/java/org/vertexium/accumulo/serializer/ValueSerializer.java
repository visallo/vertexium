package org.vertexium.accumulo.serializer;

import org.apache.accumulo.core.data.Value;

public interface ValueSerializer {
    Value objectToValue(Object value);

    <T> T valueToObject(Value value);

    <T> T valueToObject(byte[] data);
}
