package org.neolumin.vertexium.accumulo;

import org.apache.accumulo.core.data.Value;
import org.neolumin.vertexium.VertexiumException;

import java.io.IOException;
import java.nio.ByteBuffer;

// We are doing custom serialization to make this as fast as possible since this can get called many times
public class EdgeInfo implements org.neolumin.vertexium.EdgeInfo {
    public static final String CHARSET_NAME = "UTF-8";
    private final byte[] bytes;
    private transient boolean decoded;
    private transient String label;
    private transient String vertexId;

    public EdgeInfo(String label, String vertexId) {
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

    public EdgeInfo(byte[] bytes) {
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

    private void decodeBytes() {
        if (!decoded) {
            try {
                ByteBuffer in = ByteBuffer.wrap(this.bytes);
                this.label = readString(in);
                this.vertexId = readString(in);
                this.decoded = true;
            } catch (IOException ex) {
                throw new VertexiumException("Could not decode EdgeInfo data", ex);
            }
        }
    }

    private String readString(ByteBuffer in) throws IOException {
        int labelBytesLength = in.getInt();
        if (labelBytesLength == -1) {
            return null;
        } else {
            byte[] d = new byte[labelBytesLength];
            in.get(d);
            return new String(d, CHARSET_NAME);
        }
    }

    public static EdgeInfo parse(Value value) {
        return new EdgeInfo(value.get());
    }

    public Value toValue() {
        return new Value(this.bytes);
    }
}
