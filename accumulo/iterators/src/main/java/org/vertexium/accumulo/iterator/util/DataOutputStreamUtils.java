package org.vertexium.accumulo.iterator.util;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.Direction;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.model.VertexiumAccumuloIteratorException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

public class DataOutputStreamUtils {
    public static final Charset CHARSET = Charset.forName("utf8");

    public static void encodeByteSequence(DataOutputStream out, ByteSequence byteSequence) throws IOException {
        if (byteSequence == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(byteSequence.length());
        out.write(byteSequence.getBackingArray(), byteSequence.offset(), byteSequence.length());
    }

    public static void encodeByteArray(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void encodeValue(DataOutputStream out, Value value) throws IOException {
        encodeByteArray(out, value == null ? null : value.get());
    }

    public static void encodeString(DataOutputStream out, String text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = text.getBytes(CHARSET);
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    public static void encodeLong(DataOutputStream out, Long value) throws IOException {
        if (value == null) {
            out.writeByte(0x00);
            return;
        }
        out.writeByte(0x01);
        out.writeLong(value);
    }

    public static void encodeDirection(DataOutputStream out, Direction direction) throws IOException {
        switch (direction) {
            case IN:
                out.writeByte('I');
                break;
            case OUT:
                out.writeByte('O');
                break;
            default:
                throw new VertexiumAccumuloIteratorException("Unhandled direction: " + direction);
        }
    }

    public static void encodeElementType(DataOutputStream out, ElementType elementType) throws IOException {
        switch (elementType) {
            case VERTEX:
                out.writeByte('V');
                break;
            case EDGE:
                out.writeByte('E');
                break;
            default:
                throw new VertexiumAccumuloIteratorException("Unhandled elementType: " + elementType);
        }
    }
}
