package org.vertexium.accumulo.iterator.util;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.Direction;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.model.VertexiumAccumuloIteratorException;

import java.io.DataInputStream;
import java.io.IOException;

public class DataInputStreamUtils {
    public static String decodeString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return "";
        }
        byte[] data = new byte[length];
        int read = in.read(data, 0, length);
        if (read != length) {
            throw new IOException("Unexpected data length expected " + length + " found " + read);
        }
        return new String(data, DataOutputStreamUtils.CHARSET);
    }

    public static byte[] decodeByteArray(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] data = new byte[len];
        int read = in.read(data);
        if (read != len) {
            throw new IOException("Unexpected read length. Expected " + len + " found " + read);
        }
        return data;
    }

    public static ElementType decodeElementType(DataInputStream in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case 'V':
                return ElementType.VERTEX;
            case 'E':
                return ElementType.EDGE;
            default:
                throw new VertexiumAccumuloIteratorException(String.format("Unhandled elementType: 0x%02x", type));
        }
    }

    public static ByteSequence decodeByteSequence(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return new ArrayByteSequence(new byte[0]);
        }
        byte[] data = new byte[length];
        int read = in.read(data);
        if (read != length) {
            throw new IOException("Expected " + length + " bytes, found " + read + " bytes");
        }
        return new ArrayByteSequence(data);
    }

    public static Direction decodeDirection(DataInputStream in) throws IOException {
        byte direction = in.readByte();
        switch (direction) {
            case 'I':
                return Direction.IN;
            case 'O':
                return Direction.OUT;
            default:
                throw new VertexiumAccumuloIteratorException(String.format("Unhandled direction: 0x%02x", direction));
        }
    }

    public static Value decodeValue(DataInputStream in) throws IOException {
        byte[] arr = decodeByteArray(in);
        return arr == null ? null : new Value(arr);
    }

    public static Long decodeLong(DataInputStream in) throws IOException {
        byte b = in.readByte();
        if (b == 0x00) {
            return null;
        }
        return in.readLong();
    }
}
