package org.vertexium.accumulo.iterator.util;

import java.util.Arrays;

public class ByteArrayWrapper {
    private Integer cachedHashCode;
    private byte[] data;
    private int offset;
    private int length;

    public ByteArrayWrapper(byte[] data) {
        this(data, 0, data.length);
    }

    public ByteArrayWrapper(byte[] data, int offset, int length) {
        if (data == null) {
            throw new NullPointerException();
        }
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public byte[] getData() {
        if (offset == 0 && length == data.length) {
            return data;
        }
        this.data = Arrays.copyOfRange(this.data, offset, offset + length);
        this.offset = 0;
        this.length = this.data.length;
        return this.data;
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
        return ArrayUtils.equals(data, offset, length, otherWrapper.data, otherWrapper.offset, otherWrapper.length);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == null) {
            cachedHashCode = ArrayUtils.computeHash(this.data, this.offset, this.length);
        }
        return cachedHashCode;
    }
}
