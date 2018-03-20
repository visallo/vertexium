package org.vertexium.accumulo.iterator.util;

import org.apache.hadoop.io.Text;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DataInputStreamUtils {
    public static Text decodeText(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return new Text();
        }
        byte[] data = new byte[length];
        int read = in.read(data, 0, length);
        if (read != length) {
            throw new IOException("Unexpected data length expected " + length + " found " + read);
        }
        return new Text(data);
    }

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
        byte[] data = new byte[len];
        int read = in.read(data);
        if (read != len) {
            throw new IOException("Unexpected read length. Expected " + len + " found " + read);
        }
        return data;
    }

    public static int[] decodeIntArray(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        int[] data = new int[len];
        for (int i = 0; i < len; i++) {
            data[i] = in.readInt();
        }
        return data;
    }

    public static ByteArrayWrapper decodeByteArrayWrapper(DataInputStream in) throws IOException {
        byte[] result = decodeByteArray(in);
        if (result == null) {
            return null;
        }
        return new ByteArrayWrapper(result);
    }

    public static Set<String> decodeSetOfStrings(DataInputStream in) throws IOException {
        int count = in.readInt();
        Set<String> results = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            results.add(decodeString(in));
        }
        return results;
    }
}
