package org.vertexium.accumulo.serializer.kryo.quickSerializers;

import org.apache.accumulo.core.data.Value;

import java.nio.ByteBuffer;

public class DoubleQuickTypeSerializer implements QuickTypeSerializer {
    @Override
    public Value objectToValue(Object value) {
        byte[] bytes = new byte[1 + 8];
        ByteBuffer.wrap(bytes)
                .put(MARKER_DOUBLE)
                .putDouble((double) value);
        return new Value(bytes);
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.get();
        return (T) (Double) buffer.getDouble();
    }
}
