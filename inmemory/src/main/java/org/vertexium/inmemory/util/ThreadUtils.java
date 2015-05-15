package org.vertexium.inmemory.util;

import org.vertexium.VertexiumException;

public class ThreadUtils {
    public static void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            throw new VertexiumException("Could not wait", e);
        }
    }
}
