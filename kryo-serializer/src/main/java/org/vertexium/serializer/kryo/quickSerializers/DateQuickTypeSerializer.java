package org.vertexium.serializer.kryo.quickSerializers;

import java.util.Date;

public class DateQuickTypeSerializer implements QuickTypeSerializer {
    private LongQuickTypeSerializer longQuickTypeSerializer = new LongQuickTypeSerializer();

    @Override
    public byte[] objectToBytes(Object value) {
        long l = ((Date) value).getTime();
        return longQuickTypeSerializer.objectToBytes(l, MARKER_DATE);
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        return (T) new Date((long) longQuickTypeSerializer.valueToObject(data));
    }
}
