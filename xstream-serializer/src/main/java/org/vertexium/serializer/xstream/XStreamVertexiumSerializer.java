package org.vertexium.serializer.xstream;

import com.thoughtworks.xstream.XStream;
import org.vertexium.VertexiumSerializer;

public class XStreamVertexiumSerializer implements VertexiumSerializer {
    private static final XStream xstream;
    private static final byte[] EMPTY = new byte[0];

    static {
        xstream = new XStream();
        xstream.ignoreUnknownElements();
    }

    @Override
    public byte[] objectToBytes(final Object object) {
        if (object == null) {
            return EMPTY;
        }
        synchronized (object) {
            return xstream.toXML(object).getBytes();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T bytesToObject(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return (T) xstream.fromXML(new String(bytes));
    }
}
