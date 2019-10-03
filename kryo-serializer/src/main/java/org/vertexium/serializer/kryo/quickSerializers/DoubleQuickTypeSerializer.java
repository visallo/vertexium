package org.vertexium.serializer.kryo.quickSerializers;

import java.nio.ByteBuffer;

public class DoubleQuickTypeSerializer implements QuickTypeSerializer {
    @Override
    public byte[] objectToBytes(Object value) {
        byte[] bytes = new byte[1 + 8];
        ByteBuffer.wrap(bytes)
            .put(MARKER_DOUBLE)
            .putDouble((double) value);
        return bytes;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T valueToObject(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.get();
        return (T) (Double) buffer.getDouble();
    }
}
