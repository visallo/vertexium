package org.vertexium.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOUtils {
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    private IOUtils() {

    }

    public static long copy(InputStream input, OutputStream output) throws IOException {
        return copy(input, output, 0, Long.MAX_VALUE);
    }

    public static long copy(InputStream input, OutputStream output, long offset, long limit) throws IOException {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        long count = 0;
        int n;
        if (offset > 0) {
            long skipResult = input.skip(offset);
            if (skipResult < offset) {
                return 0;
            }
        }
        long len = limit;
        while (-1 != (n = input.read(buffer, 0, (int) Math.min(buffer.length, len)))) {
            output.write(buffer, 0, n);
            count += n;
            len -= n;
            if (len == 0) {
                break;
            }
        }
        return count;
    }

    public static byte[] toBytes(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out);
        return out.toByteArray();
    }

    public static byte[] toBytes(InputStream in, long offset, long limit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out, offset, limit);
        return out.toByteArray();
    }

    public static String toString(InputStream in) throws IOException {
        return new String(toBytes(in));
    }

    public static String toString(InputStream in, long offset, long limit) throws IOException {
        return new String(toBytes(in, offset, limit));
    }
}
