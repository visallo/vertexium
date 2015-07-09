package org.vertexium.accumulo.serializer.kryo;

import org.apache.accumulo.core.data.Value;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.serializer.ValueSerializer;
import org.vertexium.accumulo.serializer.kryo.quickSerializers.*;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class QuickKryoValueSerializer implements ValueSerializer {
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
    public Value objectToValue(Object value) {
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByClass.get(value.getClass());
        if (quickTypeSerializer != null) {
            return quickTypeSerializer.objectToValue(value);
        } else {
            return defaultQuickTypeSerializer.objectToValue(value);
        }
    }

    @Override
    public <T> T valueToObject(Value value) {
        return valueToObject(value.get());
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByMarker.get(data[0]);
        if (quickTypeSerializer != null) {
            return quickTypeSerializer.valueToObject(data);
        }
        throw new VertexiumException("Invalid marker: " + Integer.toHexString(data[0]));
    }
}
