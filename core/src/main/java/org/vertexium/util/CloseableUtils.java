package org.vertexium.util;

import java.io.Closeable;
import java.io.IOException;

public class CloseableUtils {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(CloseableUtils.class);

    public static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException ex) {
            LOGGER.warn("Failed to close", ex);
        }
    }

    public static void closeQuietly(Object... objects) {
        for (Object object : objects) {
            if (object instanceof Closeable) {
                closeQuietly((Closeable) object);
            }
        }
    }
}
