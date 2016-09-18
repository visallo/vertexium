package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

// We are doing custom serialization to make this as fast as possible since this can get called many times
public class EdgeInfo {
    public static final String CHARSET_NAME = "UTF-8";
    private byte[] bytes;
    private transient long timestamp;
    private transient String label;
    private transient String vertexId;
    private transient boolean decoded;

    // here for serialization
    protected EdgeInfo() {

    }

    public EdgeInfo(String label, String vertexId) {
        this(label, vertexId, System.currentTimeMillis());
    }

    public EdgeInfo(String label, String vertexId, long timestamp) {
        this.label = label;
        this.vertexId = vertexId;
        this.timestamp = timestamp;
        this.decoded = true;
    }

    public EdgeInfo(byte[] bytes, long timestamp) {
        this.timestamp = timestamp;
        this.bytes = bytes;
    }

    public String getLabel() {
        decodeBytes();
        return label;
    }

    public String getVertexId() {
        decodeBytes();
        return vertexId;
    }

    // fast access method to avoid creating a new instance of an EdgeInfo
    public static String getVertexId(Value value) {
        byte[] buffer = value.get();
        int offset = 0;

        // skip label
        int strLen = readInt(buffer, offset);
        offset += 4;
        if (strLen > 0) {
            offset += strLen;
        }

        strLen = readInt(buffer, offset);
        return readString(buffer, offset, strLen);
    }

    private void decodeBytes() {
        if (!decoded) {
            int offset = 0;

            int strLen = readInt(this.bytes, offset);
            offset += 4;
            if (strLen < 0) {
                this.label = null;
            } else {
                this.label = readString(this.bytes, offset, strLen);
                offset += strLen;
            }

            strLen = readInt(this.bytes, offset);
            offset += 4;
            if (strLen < 0) {
                this.vertexId = null;
            } else {
                this.vertexId = readString(this.bytes, offset, strLen);
            }

            this.decoded = true;
        }
    }

    public byte[] getLabelBytes() {
        // Used to use ByteBuffer here but it was to slow
        int labelBytesLength = (((int) this.bytes[0]) << 24) + (((int) this.bytes[1]) << 16) + (((int) this.bytes[2]) << 8) + ((int) this.bytes[3]);
        if (labelBytesLength == -1) {
            return null;
        }
        return Arrays.copyOfRange(this.bytes, 4, 4 + labelBytesLength);
    }


    public static EdgeInfo parse(Value value, long timestamp) {
        return new EdgeInfo(value.get(), timestamp);
    }

    public byte[] getBytes() {
        if (bytes == null) {
            try {
                byte[] labelBytes;
                int labelBytesLength;
                if (label == null) {
                    labelBytes = null;
                    labelBytesLength = -1;
                } else {
                    labelBytes = label.getBytes(CHARSET_NAME);
                    labelBytesLength = labelBytes.length;
                }

                byte[] vertexIdBytes;
                int vertexIdBytesLength;
                if (vertexId == null) {
                    vertexIdBytes = null;
                    vertexIdBytesLength = -1;
                } else {
                    vertexIdBytes = vertexId.getBytes(CHARSET_NAME);
                    vertexIdBytesLength = vertexIdBytes.length;
                }
                int len = 4 + Math.max(0, labelBytesLength) + 4 + Math.max(0, vertexIdBytesLength);

                byte[] buffer = new byte[len];
                int offset = 0;

                writeInt(labelBytesLength, buffer, offset);
                offset += 4;
                if (labelBytes != null) {
                    System.arraycopy(labelBytes, 0, buffer, offset, labelBytesLength);
                    offset += labelBytesLength;
                }

                writeInt(vertexIdBytesLength, buffer, offset);
                offset += 4;
                if (vertexIdBytes != null) {
                    System.arraycopy(vertexIdBytes, 0, buffer, offset, vertexIdBytesLength);
                }

                this.bytes = buffer;
            } catch (UnsupportedEncodingException ex) {
                throw new VertexiumAccumuloIteratorException("Could not encode edge info", ex);
            }
        }
        return bytes;
    }

    private void writeInt(int value, byte[] buffer, int offset) {
        buffer[offset++] = (byte) ((value >> 24) & 0xff);
        buffer[offset++] = (byte) ((value >> 16) & 0xff);
        buffer[offset++] = (byte) ((value >> 8) & 0xff);
        buffer[offset] = (byte) (value & 0xff);
    }

    private static int readInt(byte[] buffer, int offset) {
        return (buffer[offset] << 24)
                | (buffer[offset + 1] << 16)
                | (buffer[offset + 2] << 8)
                | (buffer[offset + 3]);
    }

    private static String readString(byte[] buffer, int offset, int length) {
        if (length < 0) {
            return null;
        }
        byte[] d = new byte[length];
        System.arraycopy(buffer, offset, d, 0, length);
        try {
            return new String(d, CHARSET_NAME);
        } catch (IOException ex) {
            throw new VertexiumAccumuloIteratorException("Could not decode edge info", ex);
        }
    }

    public Value toValue() {
        return new Value(getBytes());
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "EdgeInfo{" +
                "vertexId='" + vertexId + '\'' +
                ", label='" + label + '\'' +
                '}';
    }
}
