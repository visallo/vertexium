package org.vertexium.serializer.kryo;

import org.vertexium.VertexiumException;
import org.vertexium.VertexiumSerializer;
import org.vertexium.serializer.kryo.quickSerializers.*;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class QuickKryoVertexiumSerializer implements VertexiumSerializer {
    private static final byte[] EMPTY = new byte[0];
    private QuickTypeSerializer defaultQuickTypeSerializer = new KryoQuickTypeSerializer();
    private Map<Class, QuickTypeSerializer> quickTypeSerializersByClass = new HashMap<Class, QuickTypeSerializer>() {{
        put(String.class, new StringQuickTypeSerializer());
        put(Long.class, new LongQuickTypeSerializer());
        put(Date.class, new DateQuickTypeSerializer());
        put(Double.class, new DoubleQuickTypeSerializer());
        put(BigDecimal.class, new BigDecimalQuickTypeSerializer());
    }};
    private Map<Byte, QuickTypeSerializer> quickTypeSerializersByMarker = new HashMap<Byte, QuickTypeSerializer>() {{
        put(QuickTypeSerializer.MARKER_KRYO, new KryoQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_STRING, new StringQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_LONG, new LongQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_DATE, new DateQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_DOUBLE, new DoubleQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_BIG_DECIMAL, new BigDecimalQuickTypeSerializer());
    }};

    @Override
    public byte[] objectToBytes(Object object) {
        if (object == null) {
            return EMPTY;
        }
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByClass.get(object.getClass());
        if (quickTypeSerializer != null) {
            return quickTypeSerializer.objectToBytes(object);
        } else {
            return defaultQuickTypeSerializer.objectToBytes(object);
        }
    }

    @Override
    public <T> T bytesToObject(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByMarker.get(bytes[0]);
        if (quickTypeSerializer != null) {
            return quickTypeSerializer.valueToObject(bytes);
        }
        throw new VertexiumException("Invalid marker: " + Integer.toHexString(bytes[0]));
    }
}
