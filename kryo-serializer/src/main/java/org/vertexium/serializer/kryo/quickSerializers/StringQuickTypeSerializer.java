package org.vertexium.serializer.kryo.quickSerializers;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringQuickTypeSerializer implements QuickTypeSerializer {
    private Charset charset = StandardCharsets.UTF_8;

    @Override
    public byte[] objectToBytes(Object value) {
        byte[] valueBytes = ((String) value).getBytes(charset);
        byte[] data = new byte[1 + valueBytes.length];
        data[0] = MARKER_STRING;
        System.arraycopy(valueBytes, 0, data, 1, valueBytes.length);
        return data;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T valueToObject(byte[] data) {
        return (T) new String(data, 1, data.length - 1, charset);
    }
}
