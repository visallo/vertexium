package org.vertexium.elasticsearch5.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SaveReadInputStream extends InputStream {
    private final InputStream source;
    private ByteArrayOutputStream readBytes;

    public SaveReadInputStream(InputStream source, Long length) {
        this.source = source;
        this.readBytes = new ByteArrayOutputStream(length == null ? 1000 : length.intValue());
    }

    public String getString() {
        return new String(readBytes.toByteArray());
    }

    @Override
    public int read() throws IOException {
        int b = source.read();
        if (b >= 0) {
            readBytes.write(b);
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = source.read(b, off, len);
        if (read >= 0) {
            readBytes.write(b, off, read);
        }
        return read;
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.source.close();
    }
}
