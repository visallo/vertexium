package org.vertexium.inmemory.util;

public class IncreasingTime {
    private static long last = System.currentTimeMillis();

    public static synchronized long currentTimeMillis() {
        long now = System.currentTimeMillis();
        if (now > last) {
            last = now;
        } else {
            last++;
        }
        return last;
    }
}
