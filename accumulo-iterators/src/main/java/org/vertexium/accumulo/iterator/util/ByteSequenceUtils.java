package org.vertexium.accumulo.iterator.util;

import org.apache.accumulo.core.data.ByteSequence;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteSequenceUtils {
    public static boolean equals(ByteSequence byteSequence, byte[] bytes) {
        if (byteSequence.length() != bytes.length) {
            return false;
        }
        if (byteSequence.isBackedByArray() && byteSequence.offset() == 0) {
            return Arrays.equals(bytes, byteSequence.getBackingArray());
        }

        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != byteSequence.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static String toString(ByteSequence byteSequence) {
        return new String(
                byteSequence.getBackingArray(),
                byteSequence.offset(),
                byteSequence.length(),
                StandardCharsets.UTF_8
        );
    }

    public static int indexOf(ByteSequence bytes, byte b) {
        return indexOf(bytes, b, 0);
    }

    public static int indexOf(ByteSequence bytes, byte b, int startIndex) {
        // TODO utf-8 encoding issues?
        for (int i = startIndex; i < bytes.length(); i++) {
            if (bytes.byteAt(i) == b) {
                return i;
            }
        }
        return -1;
    }

    public static ByteSequence subSequence(ByteSequence bytes, int startIndex) {
        return subSequence(bytes, startIndex, bytes.length());
    }

    public static ByteSequence subSequence(ByteSequence bytes, int startIndex, int endIndex) {
        return bytes.subSequence(startIndex, endIndex);
    }

    public static void putIntoByteBuffer(ByteSequence bytes, ByteBuffer bb) {
        bb.put(bytes.getBackingArray(), bytes.offset(), bytes.length());
    }

    public static byte[] getBytes(ByteSequence byteSequence) {
        return byteSequence.toArray();
    }
}
