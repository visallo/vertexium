package org.vertexium.serializer.kryo.quickSerializers;

public class LongQuickTypeSerializer implements QuickTypeSerializer {
    @Override
    public byte[] objectToBytes(Object value) {
        long time = (long) value;
        return objectToBytes(time, MARKER_LONG);
    }

    public byte[] objectToBytes(long time, byte marker) {
        byte[] results = new byte[1 + 8];
        results[0] = marker;
        results[1] = (byte) (time >>> 56);
        results[2] = (byte) (time >>> 48);
        results[3] = (byte) (time >>> 40);
        results[4] = (byte) (time >>> 32);
        results[5] = (byte) (time >>> 24);
        results[6] = (byte) (time >>> 16);
        results[7] = (byte) (time >>> 8);
        results[8] = (byte) (time >>> 0);
        return results;
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        long l = ((((long) data[1]) << 56)
            | (((long) data[2] & 0xff) << 48)
            | (((long) data[3] & 0xff) << 40)
            | (((long) data[4] & 0xff) << 32)
            | (((long) data[5] & 0xff) << 24)
            | (((long) data[6] & 0xff) << 16)
            | (((long) data[7] & 0xff) << 8)
            | (((long) data[8] & 0xff)));
        return (T) (Long) l;
    }
}
