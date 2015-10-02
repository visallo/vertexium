package org.vertexium.serializer.kryo.quickSerializers;

public interface QuickTypeSerializer {
    byte MARKER_KRYO = 0;
    byte MARKER_STRING = 1;
    byte MARKER_LONG = 2;
    byte MARKER_DATE = 3;
    byte MARKER_DOUBLE = 4;
    byte MARKER_BIG_DECIMAL = 5;

    byte[] objectToBytes(Object value);

    <T> T valueToObject(byte[] data);
}
