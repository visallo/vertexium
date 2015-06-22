package org.vertexium.accumulo.serializer.kryo.quickSerializers;

import org.apache.accumulo.core.data.Value;

import java.util.Date;

public class DateQuickTypeSerializer implements QuickTypeSerializer {
    private LongQuickTypeSerializer longQuickTypeSerializer = new LongQuickTypeSerializer();

    @Override
    public Value objectToValue(Object value) {
        long l = ((Date) value).getTime();
        return longQuickTypeSerializer.objectToValue(l, MARKER_DATE);
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        return (T) new Date((long) longQuickTypeSerializer.valueToObject(data));
    }
}
