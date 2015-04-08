package org.vertexium.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class CloseableUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloseableUtils.class);

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
}
