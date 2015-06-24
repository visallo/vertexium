package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Value;
import org.vertexium.VertexiumException;
import org.vertexium.id.NameSubstitutionStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;

// We are doing custom serialization to make this as fast as possible since this can get called many times
public class EdgeInfo implements org.vertexium.EdgeInfo {
    public static final String CHARSET_NAME = "UTF-8";
    private final byte[] bytes;
    private final NameSubstitutionStrategy nameSubstitutionStrategy;
    private transient long timestamp;
    private transient boolean decoded;
    private transient String label;
    private transient String vertexId;

    public EdgeInfo(String label, String vertexId, NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.timestamp = System.currentTimeMillis();
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;

        try {
            byte[] labelBytes;
            int labelBytesLength;
            if (label == null) {
                labelBytes = null;
                labelBytesLength = -1;
            } else {
                labelBytes = nameSubstitutionStrategy.deflate(label).getBytes(CHARSET_NAME);
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
        } catch (IOException ex) {
            throw new VertexiumException("Could not serialize edge info", ex);
        }
    }

    public EdgeInfo(byte[] bytes, long timestamp, NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.timestamp = timestamp;
        this.bytes = bytes;
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
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
        try {
            ByteBuffer in = ByteBuffer.wrap(value.get());
            readString(in); // skip label
            return readString(in);
        } catch (IOException ex) {
            throw new VertexiumException("Could not decode EdgeInfo data", ex);
        }
    }

    private void decodeBytes() {
        if (!decoded) {
            try {
                ByteBuffer in = ByteBuffer.wrap(this.bytes);
                this.label = nameSubstitutionStrategy.inflate(readString(in));
                this.vertexId = readString(in);
                this.decoded = true;
            } catch (IOException ex) {
                throw new VertexiumException("Could not decode EdgeInfo data", ex);
            }
        }
    }

    private static String readString(ByteBuffer in) throws IOException {
        int labelBytesLength = in.getInt();
        if (labelBytesLength == -1) {
            return null;
        } else {
            byte[] d = new byte[labelBytesLength];
            in.get(d);
            return new String(d, CHARSET_NAME);
        }
    }

    public static EdgeInfo parse(Value value, long timestamp, NameSubstitutionStrategy nameSubstitutionStrategy) {
        return new EdgeInfo(value.get(), timestamp, nameSubstitutionStrategy);
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
