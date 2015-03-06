package org.neolumin.vertexium.test.util;

import java.io.IOException;
import java.io.InputStream;

public class LargeStringInputStream extends InputStream {
    private final int size;
    private int current;
    private int left;

    public LargeStringInputStream(int size) {
        this.size = size;
        this.current = 'A';
        this.left = this.size;
    }

    @Override
    public int read() throws IOException {
        int ret = this.current;
        if (this.left <= 0) {
            return -1;
        }
        this.left--;
        this.current = this.current + 1;
        if (this.current > 'Z') {
            this.current = 'A';
        }
        return ret;
    }
}
