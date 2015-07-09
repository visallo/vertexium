package org.vertexium.accumulo.serializer.kryo.quickSerializers;

import org.apache.accumulo.core.data.Value;

import java.math.BigDecimal;

public class BigDecimalQuickTypeSerializer implements QuickTypeSerializer {
    @Override
    public Value objectToValue(Object value) {
        BigDecimal valueAsBigDecimal = (BigDecimal) value;
        byte[] valueBytes = valueAsBigDecimal.toString().getBytes();
        byte[] data = new byte[1 + valueBytes.length];
        data[0] = MARKER_BIG_DECIMAL;
        System.arraycopy(valueBytes, 0, data, 1, valueBytes.length);
        return new Value(data);
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        return (T) new BigDecimal(new String(data, 1, data.length - 1));
    }
}
