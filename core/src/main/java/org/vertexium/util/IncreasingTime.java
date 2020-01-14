package org.vertexium.util;

import org.vertexium.VertexiumException;

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

    public static void advanceTime(int inc) {
        last += inc;
    }

    public static void catchUp() {
        advanceTime(1);
        while (last > System.currentTimeMillis()) {
            try {
                Thread.sleep(Math.max(0, last - System.currentTimeMillis()));
            } catch (InterruptedException e) {
                throw new VertexiumException("Interrupted waiting for catch up", e);
            }
        }
    }
}
