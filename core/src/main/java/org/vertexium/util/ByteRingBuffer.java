package org.vertexium.util;

import static com.google.common.base.Preconditions.checkArgument;

public class ByteRingBuffer {
    private byte[] buffer;
    private int position;
    private int used;

    public ByteRingBuffer(int size) {
        checkArgument(size > 0, "size must be greater than 0");
        buffer = new byte[size];
    }

    public void resize(int newSize) {
        checkArgument(newSize >= getUsed(), "cannot resize smaller than data used");
        byte[] newBuffer = new byte[newSize];
        int newUsed = read(newBuffer, 0, newSize);
        buffer = newBuffer;
        position = 0;
        used = newUsed;
    }

    public int getSize() {
        return buffer.length;
    }

    public int getFree() {
        return getSize() - getUsed();
    }

    public int getUsed() {
        return used;
    }

    public void clear() {
        position = 0;
        used = 0;
    }

    public int write(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >= 0");
        checkArgument(len <= getFree(), "out of free space");
        if (used == 0) {
            position = 0;
        }
        int p1 = position + used;
        if (p1 < buffer.length) { // free space in two pieces
            int part1Len = Math.min(len, buffer.length - p1);
            append(buf, pos, part1Len);
            int part2Len = Math.min(len - part1Len, position);
            append(buf, pos + part1Len, part2Len);
            return part1Len + part2Len;
        } else { // free space in one piece
            append(buf, pos, len);
            return len;
        }
    }

    public int write(byte[] buf) {
        return write(buf, 0, buf.length);
    }

    private void append(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >= 0");
        if (len == 0) {
            return;
        }
        int p = clip(position + used);
        System.arraycopy(buf, pos, buffer, p, len);
        used += len;
    }

    public int read(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >=0");
        if (getUsed() == 0) {
            return -1;
        }
        int part1Length = Math.min(len, Math.min(used, buffer.length - position));
        remove(buf, pos, part1Length);
        int part2Length = Math.min(len - part1Length, used);
        remove(buf, pos + part1Length, part2Length);
        return part1Length + part2Length;
    }

    public int read(byte[] buf) {
        return read(buf, 0, buf.length);
    }

    public int read() {
        if (getUsed() < 1) {
            return -1;
        }
        byte[] buffer = new byte[1];
        read(buffer);
        return buffer[0];
    }

    private void remove(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >= 0");
        if (len == 0) {
            return;
        }
        System.arraycopy(buffer, position, buf, pos, len);
        position = clip(position + len);
        used -= len;
    }

    private int clip(int p) {
        return (p < buffer.length) ? p : (p - buffer.length);
    }
}
