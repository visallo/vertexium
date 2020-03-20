package org.vertexium.accumulo.models;

import org.apache.accumulo.core.data.Value;

import java.nio.charset.StandardCharsets;

// We are doing custom serialization to make this as fast as possible since this can get called many times
public class AccumuloEdgeInfo {
    private transient long timestamp;
    private transient String label;
    private transient String vertexId;

    public AccumuloEdgeInfo(String label, String vertexId) {
        this(label, vertexId, System.currentTimeMillis(), true);
    }

    public AccumuloEdgeInfo(String label, String vertexId, long timestamp, boolean includeEdgeVertexIds) {
        if (label == null) {
            throw new IllegalArgumentException("label cannot be null");
        }
        if (includeEdgeVertexIds && vertexId == null) {
            throw new IllegalArgumentException("vertexId cannot be null");
        }
        this.label = label;
        this.vertexId = vertexId;
        this.timestamp = timestamp;
    }

    public static AccumuloEdgeInfo parse(byte[] bytes, long timestamp) {
        int offset = 0;

        int strLen = readInt(bytes, offset);
        offset += Integer.BYTES;
        String label = readString(bytes, offset, strLen);
        offset += strLen;

        strLen = readInt(bytes, offset);
        offset += Integer.BYTES;
        String vertexId = readString(bytes, offset, strLen);

        return new AccumuloEdgeInfo(label, vertexId, timestamp, false);
    }

    public String getLabel() {
        return label;
    }

    public String getVertexId() {
        return vertexId;
    }

    private void writeInt(int value, byte[] buffer, int offset) {
        buffer[offset++] = (byte) ((value >> 24) & 0xff);
        buffer[offset++] = (byte) ((value >> 16) & 0xff);
        buffer[offset++] = (byte) ((value >> 8) & 0xff);
        buffer[offset] = (byte) (value & 0xff);
    }

    private static int readInt(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xff) << 24)
            | ((buffer[offset + 1] & 0xff) << 16)
            | ((buffer[offset + 2] & 0xff) << 8)
            | ((buffer[offset + 3] & 0xff));
    }

    private static String readString(byte[] buffer, int offset, int length) {
        byte[] d = new byte[length];
        System.arraycopy(buffer, offset, d, 0, length);
        return new String(d, StandardCharsets.UTF_8);
    }

    public Value toValue() {
        byte[] labelBytes = label.getBytes(StandardCharsets.UTF_8);
        int labelBytesLength = labelBytes.length;

        byte[] vertexIdBytes = vertexId.getBytes(StandardCharsets.UTF_8);
        int vertexIdBytesLength = vertexIdBytes.length;
        int len = Integer.BYTES + labelBytesLength + Integer.BYTES + vertexIdBytesLength;

        byte[] buffer = new byte[len];
        int offset = 0;

        writeInt(labelBytesLength, buffer, offset);
        offset += Integer.BYTES;
        System.arraycopy(labelBytes, 0, buffer, offset, labelBytesLength);
        offset += labelBytesLength;

        writeInt(vertexIdBytesLength, buffer, offset);
        offset += Integer.BYTES;
        System.arraycopy(vertexIdBytes, 0, buffer, offset, vertexIdBytesLength);

        return new Value(buffer);
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format(
            "AccumuloEdgeInfo{, timestamp=%d, label='%s', vertexId='%s'}",
            timestamp,
            label,
            vertexId
        );
    }
}
