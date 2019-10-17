package org.vertexium.accumulo.iterator.util;

import java.util.Arrays;

public class ByteArrayWrapper {
    private Integer cachedHashCode;
    private final byte[] data;

    public ByteArrayWrapper(byte[] data) {
        if (data == null) {
            throw new NullPointerException();
        }
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ByteArrayWrapper)) {
            return false;
        }
        ByteArrayWrapper otherWrapper = (ByteArrayWrapper) other;
        if (hashCode() != otherWrapper.hashCode()) {
            return false;
        }
        return Arrays.equals(data, otherWrapper.data);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == null) {
            cachedHashCode = Arrays.hashCode(data);
        }
        return cachedHashCode;
    }
}
