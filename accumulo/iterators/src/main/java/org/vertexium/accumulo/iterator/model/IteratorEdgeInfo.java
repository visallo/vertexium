package org.vertexium.accumulo.iterator.model;

import org.vertexium.accumulo.iterator.util.ByteArrayWrapper;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

// We are doing custom serialization to make this as fast as possible since this can get called many times
public class IteratorEdgeInfo {
    private transient long timestamp;
    private transient int labelIndex;
    private transient byte[] vertexIdBytes;

    /**
     * This constructor will only borrow the byte array so it can be reused
     */
    public IteratorEdgeInfo(EdgeLabels edgeLabels, byte[] bytes, long timestamp) {
        this.timestamp = timestamp;

        int offset = 0;

        int strLen = readInt(bytes, offset);
        offset += Integer.BYTES;
        if (edgeLabels == null) {
            this.labelIndex = -1;
        } else {
            this.labelIndex = edgeLabels.add(bytes, offset, strLen);
        }
        offset += strLen;

        strLen = readInt(bytes, offset);
        offset += Integer.BYTES;
        this.vertexIdBytes = Arrays.copyOfRange(bytes, offset, offset + strLen);
    }

    public static ByteArrayWrapper parseVertexIdBytes(byte[] bytes) {
        int offset = 0;

        int strLen = readInt(bytes, offset);
        offset += Integer.BYTES;
        offset += strLen;

        strLen = readInt(bytes, offset);
        offset += Integer.BYTES;
        return new ByteArrayWrapper(bytes, offset, strLen);
    }

    public int getLabelIndex() {
        return labelIndex;
    }

    public byte[] getVertexIdBytes() {
        return vertexIdBytes;
    }

    private static int readInt(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xff) << 24)
            | ((buffer[offset + 1] & 0xff) << 16)
            | ((buffer[offset + 2] & 0xff) << 8)
            | ((buffer[offset + 3] & 0xff));
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format(
            "IteratorEdgeInfo{timestamp=%d, labelIndex=%d, vertexIdBytes=%s}",
            timestamp,
            labelIndex,
            new String(vertexIdBytes, StandardCharsets.UTF_8)
        );
    }
}
