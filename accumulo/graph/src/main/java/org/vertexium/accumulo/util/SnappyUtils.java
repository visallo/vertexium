package org.vertexium.accumulo.util;

import org.vertexium.VertexiumException;
import org.vertexium.util.IOUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class SnappyUtils {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SnappyUtils.class);

    public static boolean testSnappySupport() {
        try {
            String testString = "Hello World";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            SnappyOutputStream out = new SnappyOutputStream(baos);
            out.write(testString.getBytes());
            out.close();
            byte[] bytes = baos.toByteArray();

            ByteArrayInputStream bain = new ByteArrayInputStream(bytes);
            SnappyInputStream in = new SnappyInputStream(bain);
            String result = new String(IOUtils.toBytes(in));
            if (!result.equals(testString)) {
                throw new VertexiumException("uncompressed string did not match compressed string");
            }
            return true;
        } catch (Throwable ex) {
            LOGGER.error("Could not verify support for snappy compression", ex);
            return false;
        }
    }
}
