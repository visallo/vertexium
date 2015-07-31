package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

// We are doing custom serialization to make this as fast as possible since this can get called many times
public class EdgeInfo {
    public static final String CHARSET_NAME = "UTF-8";
    private final byte[] bytes;
    private transient long timestamp;
    private transient boolean decoded;
    private transient String label;
    private transient String vertexId;

    public EdgeInfo(String label, String vertexId) {
        this(label, vertexId, System.currentTimeMillis());
    }

    public EdgeInfo(String label, String vertexId, long timestamp) {
        try {
            this.timestamp = timestamp;

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
            int len = 4 + labelBytesLength + 4 + vertexIdBytesLength;

            ByteBuffer buffer = ByteBuffer.allocate(len);
            buffer.putInt(labelBytesLength);
            if (labelBytes != null) {
                buffer.put(labelBytes);
            }
            buffer.putInt(vertexIdBytesLength);
            if (vertexIdBytes != null) {
                buffer.put(vertexIdBytes);
            }

            this.bytes = buffer.array();
        } catch (UnsupportedEncodingException ex) {
            throw new VertexiumAccumuloIteratorException("Could not encode edge info", ex);
        }
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
        ByteBuffer in = ByteBuffer.wrap(value.get());
        readString(in); // skip label
        return readString(in);
    }

    private void decodeBytes() {
        if (!decoded) {
            ByteBuffer in = ByteBuffer.wrap(this.bytes);
            this.label = readString(in);
            this.vertexId = readString(in);
            this.decoded = true;
        }
    }

    public byte[] getLabelBytes() {
        ByteBuffer in = ByteBuffer.wrap(this.bytes);
        int labelBytesLength = in.getInt();
        if (labelBytesLength == -1) {
            return null;
        }
        byte[] d = new byte[labelBytesLength];
        in.get(d);
        return d;
    }

    private static String readString(ByteBuffer in) {
        int labelBytesLength = in.getInt();
        if (labelBytesLength == -1) {
            return null;
        } else {
            byte[] d = new byte[labelBytesLength];
            in.get(d);
            try {
                return new String(d, CHARSET_NAME);
            } catch (IOException ex) {
                throw new VertexiumAccumuloIteratorException("Could not decode edge info", ex);
            }
        }
    }

    public static EdgeInfo parse(Value value, long timestamp) {
        return new EdgeInfo(value.get(), timestamp);
    }

    public Value toValue() {
        return new Value(this.bytes);
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
