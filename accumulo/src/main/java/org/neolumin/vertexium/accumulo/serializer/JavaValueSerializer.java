package org.neolumin.vertexium.accumulo.serializer;

import org.apache.accumulo.core.data.Value;
import org.neolumin.vertexium.util.JavaSerializableUtils;

public class JavaValueSerializer implements ValueSerializer {
    @Override
    public Value objectToValue(Object value) {
        return new Value(JavaSerializableUtils.objectToBytes(value));
    }

    @Override
    public <T> T valueToObject(Value value) {
        return valueToObject(value.get());
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        return (T) JavaSerializableUtils.bytesToObject(data);
    }
}
